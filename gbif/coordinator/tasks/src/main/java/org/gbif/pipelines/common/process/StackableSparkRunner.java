package org.gbif.pipelines.common.process;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kubernetes.client.openapi.ApiException;
import java.util.*;
import java.util.function.Consumer;
import javax.validation.constraints.Size;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.configs.DistributedConfiguration;
import org.gbif.pipelines.common.configs.SparkConfiguration;
import org.gbif.stackable.ConfigUtils;
import org.gbif.stackable.K8StackableSparkController;
import org.gbif.stackable.SparkCrd;
import org.gbif.stackable.SparkCrd.Config;
import org.gbif.stackable.SparkCrd.Executor;
import org.gbif.stackable.SparkCrd.PodOverrides;
import org.gbif.stackable.SparkCrd.PodOverrides.Metadata.Annotations;
import org.gbif.stackable.SparkCrd.PodOverrides.Metadata.Annotations.TaskGroup;
import org.gbif.stackable.SparkCrd.PodOverrides.Metadata.Annotations.TaskGroup.MinResource;
import org.gbif.stackable.SparkCrd.Resources;
import org.gbif.stackable.SparkCrd.Resources.Memory;

/** Class to build an instance of ProcessBuilder for direct or spark command */
@SuppressWarnings("all")
@Slf4j
public final class StackableSparkRunner {

  private static final ObjectMapper MAPPER = new ObjectMapper(new JsonFactory());

  private static final String DELIMITER = " ";

  @NonNull private final String kubeConfigFile;

  @Builder.Default private Consumer<StringJoiner> beamConfigFn = j -> {};

  @NonNull private final String sparkCrdConfigFile;

  @NonNull private final DistributedConfiguration distributedConfig;

  @Getter @NonNull private final String sparkAppName;

  @NonNull private final SparkSettings sparkSettings;

  private final K8StackableSparkController k8StackableSparkController;

  @Builder.Default private final int sleepTimeInMills = 1_000;

  private AbstractMap<String, Object> sparkApplicationData;

  private boolean deleteOnFinish;

  @Getter private SparkCrd sparkCrd;

  @Builder
  public StackableSparkRunner(
      @NonNull String kubeConfigFile,
      @NonNull String sparkCrdConfigFile,
      @NonNull SparkConfiguration sparkConfig,
      @NonNull DistributedConfiguration distributedConfig,
      @NonNull @Size(min = 10, max = 63) String sparkAppName,
      @NonNull SparkSettings sparkSettings,
      @NonNull Consumer<StringJoiner> beamConfigFn,
      @NonNull boolean deleteOnFinish) {
    this.kubeConfigFile = kubeConfigFile;
    this.sparkCrdConfigFile = sparkCrdConfigFile;
    this.distributedConfig = distributedConfig;
    this.sparkAppName = normalize(sparkAppName);
    this.sparkSettings = sparkSettings;
    this.beamConfigFn = beamConfigFn;
    this.sparkCrd = loadSparkCrd();
    this.k8StackableSparkController =
        K8StackableSparkController.builder()
            .kubeConfig(ConfigUtils.loadKubeConfig(kubeConfigFile))
            .sparkCrd(sparkCrd)
            .build();
    this.deleteOnFinish = deleteOnFinish;
  }

  public StackableSparkRunner start() {
    log.info("Submitting Spark Application {}", sparkAppName);
    try {
      sparkApplicationData = k8StackableSparkController.submitSparkApplication(sparkAppName);
    } catch (ApiException ex) {
      log.error("K8s API error: {}", ex.getResponseBody());
      throw new PipelinesException(ex);
    }
    return this;
  }

  @SneakyThrows
  public int waitFor() {

    while (!hasFinished()) {
      Thread.currentThread().sleep(sleepTimeInMills);
    }

    K8StackableSparkController.Phase phase =
        k8StackableSparkController.getApplicationPhase(sparkAppName);

    log.info("Spark Application {}, finished with status {}", sparkAppName, phase);

    if (deleteOnFinish) {
      k8StackableSparkController.stopSparkApplication(sparkAppName);
    }

    if (K8StackableSparkController.Phase.FAILED == phase) {
      return -1;
    }
    return 0;
  }

  private SparkCrd loadSparkCrd() {
    SparkCrd sparkCrd = ConfigUtils.loadSparkCdr(sparkCrdConfigFile);
    SparkCrd crd =
        sparkCrd.toBuilder()
            .metadata(sparkCrd.getMetadata().builder().name(sparkAppName).build())
            .spec(
                sparkCrd.getSpec().toBuilder()
                    .mainClass(distributedConfig.mainClass)
                    .mainApplicationFile(distributedConfig.jarPath)
                    .args(buildArgs())
                    .sparkConf(mergeSparkConfSettings(sparkCrd.getSpec().getSparkConf()))
                    .executor(mergeExecutorSettings(sparkCrd.getSpec().getExecutor()))
                    .build())
            .build();

    log.debug("SparkCrd: {}", crd.toString());

    return crd;
  }

  private List<String> buildArgs() {
    StringJoiner joiner = new StringJoiner(DELIMITER);
    beamConfigFn.accept(joiner);
    return Arrays.asList(joiner.toString().split(DELIMITER));
  }

  /**
   * A lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.'.
   * Must start and end with an alphanumeric character and its max lentgh is 64 characters.
   */
  private static String normalize(String sparkAppName) {
    return sparkAppName.toLowerCase().replace("_to_", "-").replace("_", "-");
  }

  @SneakyThrows
  private SparkCrd.Executor mergeExecutorSettings(SparkCrd.Executor executor) {

    String memoryLimit = String.valueOf(sparkSettings.getExecutorMemory()) + "Gi";

    Executor updatedExecutor = executor.toBuilder().build();
    if (executor.getReplicas() != null) {
      updatedExecutor.setReplicas(sparkSettings.getExecutorNumbers());
    }

    // Update yunikorn taskGroups settings
    PodOverrides podOverrides = updatedExecutor.getPodOverrides();
    if (podOverrides != null && podOverrides.getMetadata() != null) {
      String taskGroups = podOverrides.getMetadata().getAnnotations().getTaskGroups();
      TaskGroup updatedTaskGroups =
          MAPPER.readValue(taskGroups, new TypeReference<List<TaskGroup>>() {}).get(0).toBuilder()
              .minMember(String.valueOf(sparkSettings.getExecutorNumbers()))
              .minResource(
                  MinResource.builder()
                      .cpu(executor.getConfig().getResources().getCpu().getMin())
                      .memory(memoryLimit)
                      .build())
              .build();

      Annotations updatedAnnotations =
          podOverrides.getMetadata().getAnnotations().toBuilder()
              .taskGroups(MAPPER.writeValueAsString(Collections.singletonList(updatedTaskGroups)))
              .build();

      podOverrides.getMetadata().setAnnotations(updatedAnnotations);
    }

    Resources updatedResources =
        executor.getConfig().getResources().toBuilder()
            .memory(Memory.builder().limit(memoryLimit).build())
            .build();

    if (updatedExecutor.getConfig() != null) {
      updatedExecutor.getConfig().setResources(updatedResources);
    } else {
      updatedExecutor.setConfig(Config.builder().resources(updatedResources).build());
    }

    return updatedExecutor;
  }

  private Map<String, String> mergeSparkConfSettings(Map<String, String> sparkConf) {

    Map<String, String> newSparkConf = new HashMap<>(sparkConf);

    if (sparkSettings.getParallelism() < 1) {
      throw new IllegalArgumentException("sparkParallelism can't be 0");
    }

    newSparkConf.computeIfAbsent(
        "spark.dynamicAllocation.maxExecutors",
        (key) -> String.valueOf(sparkSettings.getExecutorNumbers()));

    newSparkConf.computeIfAbsent(
        "spark.dynamicAllocation.initialExecutors",
        (key) -> String.valueOf(sparkSettings.getExecutorNumbers()));

    newSparkConf.put("spark.default.parallelism", String.valueOf(sparkSettings.getParallelism()));

    return newSparkConf;
  }

  @SneakyThrows
  private boolean hasFinished() {
    K8StackableSparkController.Phase phase =
        k8StackableSparkController.getApplicationPhase(sparkAppName);
    return K8StackableSparkController.Phase.SUCCEEDED == phase
        || K8StackableSparkController.Phase.FAILED == phase;
  }
}
