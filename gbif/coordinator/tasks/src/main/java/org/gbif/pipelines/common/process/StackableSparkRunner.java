package org.gbif.pipelines.common.process;

import java.util.*;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.MainSparkSettings;
import org.gbif.pipelines.common.configs.DistributedConfiguration;
import org.gbif.pipelines.common.configs.SparkConfiguration;
import org.gbif.stackable.ConfigUtils;
import org.gbif.stackable.K8StackableSparkController;
import org.gbif.stackable.SparkCrd;

/** Class to build an instance of ProcessBuilder for direct or spark command */
@SuppressWarnings("all")
@Slf4j
public final class StackableSparkRunner {
  private static final String DELIMITER = " ";

  @NonNull private final String kubeConfigFile;

  @Builder.Default private Consumer<StringJoiner> beamConfigFn = j -> {};

  @NonNull private final String sparkCrdConfigFile;

  @NonNull private final SparkConfiguration sparkConfig;

  @NonNull private final DistributedConfiguration distributedConfig;

  @NonNull private final String sparkAppName;

  @NonNull private final MainSparkSettings sparkSettings;

  private final K8StackableSparkController k8StackableSparkController;

  @Builder.Default private final int sleepTimeInMills = 1_000;

  private AbstractMap<String, Object> sparkApplicationData;

  @Builder
  public StackableSparkRunner(
      @NonNull String kubeConfigFile,
      @NonNull String sparkCrdConfigFile,
      @NonNull SparkConfiguration sparkConfig,
      @NonNull DistributedConfiguration distributedConfig,
      @NonNull String sparkAppName,
      @NonNull MainSparkSettings sparkSettings,
      @NonNull Consumer<StringJoiner> beamConfigFn) {
    this.kubeConfigFile = kubeConfigFile;
    this.sparkCrdConfigFile = sparkCrdConfigFile;
    this.sparkConfig = sparkConfig;
    this.distributedConfig = distributedConfig;
    this.sparkAppName = sparkAppName;
    this.sparkSettings = sparkSettings;
    this.beamConfigFn = beamConfigFn;
    this.k8StackableSparkController =
        K8StackableSparkController.builder()
            .kubeConfig(ConfigUtils.loadKubeConfig(kubeConfigFile))
            .sparkCrd(loadSparkCrd())
            .build();
  }

  private SparkCrd.Spec.Resources mergeDriverResources(SparkCrd.Spec.Resources resources) {
    return resources
        .toBuilder()
        .memory(resources.getMemory().toBuilder().limit(sparkConfig.driverMemory).build())
        .build();
  }

  private SparkCrd.Spec.Resources mergeExecutorResources(SparkCrd.Spec.Resources resources) {
    return resources
        .toBuilder()
        .memory(
            resources
                .getMemory()
                .toBuilder()
                .limit(String.valueOf(sparkSettings.getExecutorMemory()) + "Gi")
                .build())
        .cpu(resources.getCpu().builder().max(String.valueOf(sparkConfig.executorCores)).build())
        .build();
  }

  private SparkCrd.Spec.Driver mergeDriverSettings(SparkCrd.Spec.Driver driver) {
    return driver.toBuilder().resources(mergeDriverResources(driver.getResources())).build();
  }

  private SparkCrd.Spec.Executor mergeExecutorSettings(SparkCrd.Spec.Executor executor) {
    return executor
        .toBuilder()
        .resources(mergeExecutorResources(executor.getResources()))
        .instances(sparkSettings.getExecutorNumbers())
        .build();
  }

  private Map<String, String> mergeSparkConfSettings(Map<String, String> sparkConf) {

    Map<String, String> newSparkConf = new HashMap<>(sparkConf);

    Optional.ofNullable(distributedConfig.metricsPropertiesPath)
        .ifPresent(x -> newSparkConf.put("spark.metrics.conf", x));
    Optional.ofNullable(distributedConfig.extraClassPath)
        .ifPresent(x -> newSparkConf.put("spark.driver.extraClassPath", x));
    Optional.ofNullable(distributedConfig.driverJavaOptions)
        .ifPresent(x -> newSparkConf.put("driver-java-options", x));

    if (sparkSettings.getParallelism() < 1) {
      throw new IllegalArgumentException("sparkParallelism can't be 0");
    }

    newSparkConf.put("spark.default.parallelism", String.valueOf(sparkSettings.getParallelism()));
    newSparkConf.put("spark.executor.memoryOverhead", String.valueOf(sparkConfig.memoryOverhead));
    newSparkConf.put("spark.dynamicAllocation.enabled", "false");

    return newSparkConf;
  }

  public List<String> buildArgs() {
    StringJoiner joiner = new StringJoiner(DELIMITER);
    beamConfigFn.accept(joiner);
    return Arrays.asList(joiner.toString().split(DELIMITER));
  }

  private SparkCrd loadSparkCrd() {
    SparkCrd sparkCrd = ConfigUtils.loadSparkCdr(sparkCrdConfigFile);
    return sparkCrd
        .toBuilder()
        .metadata(sparkCrd.getMetadata().builder().name(sparkAppName).build())
        .spec(
            sparkCrd
                .getSpec()
                .toBuilder()
                .mainClass(distributedConfig.mainClass)
                .mainApplicationFile(distributedConfig.jarPath)
                .args(buildArgs())
                .driver(mergeDriverSettings(sparkCrd.getSpec().getDriver()))
                .sparkConf(mergeSparkConfSettings(sparkCrd.getSpec().getSparkConf()))
                .executor(mergeExecutorSettings(sparkCrd.getSpec().getExecutor()))
                .build())
        .build();
  }

  public StackableSparkRunner start() {
    sparkApplicationData = k8StackableSparkController.submitSparkApplication(sparkAppName);
    return this;
  }

  @SneakyThrows
  public int waitFor() {
    while (!hasFinished()) {
      Thread.currentThread().sleep(sleepTimeInMills);
    }

    K8StackableSparkController.Phase phase =
        k8StackableSparkController.getApplicationPhase(sparkAppName);

    if (K8StackableSparkController.Phase.FAILED == phase) {
      return -1;
    }
    return 0;
  }

  private boolean hasFinished() {
    K8StackableSparkController.Phase phase =
        k8StackableSparkController.getApplicationPhase(sparkAppName);
    return K8StackableSparkController.Phase.SUCCEEDED == phase
        || K8StackableSparkController.Phase.FAILED == phase;
  }
}
