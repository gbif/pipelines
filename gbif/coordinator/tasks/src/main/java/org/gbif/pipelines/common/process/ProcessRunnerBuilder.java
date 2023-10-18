package org.gbif.pipelines.common.process;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.configs.DistributedConfiguration;
import org.gbif.pipelines.common.configs.SparkConfiguration;

/** Class to build an instance of ProcessBuilder for direct or spark command */
@SuppressWarnings("all")
@Slf4j
@Builder
public final class ProcessRunnerBuilder {
  private static final String DELIMITER = " ";

  @NonNull private SparkConfiguration sparkConfig;
  @NonNull private DistributedConfiguration distributedConfig;
  @Builder.Default private Consumer<StringJoiner> beamConfigFn = j -> {};
  @Getter private String sparkAppName;
  private SparkSettings sparkSettings;

  public ProcessBuilder get() {
    return buildSpark();
  }

  public String[] buildOptions() {
    StringJoiner joiner = new StringJoiner(DELIMITER);
    beamConfigFn.accept(joiner);
    return joiner.toString().split(DELIMITER);
  }

  /** Builds ProcessBuilder to process spark command */
  private ProcessBuilder buildSpark() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("spark2-submit");

    Optional.ofNullable(distributedConfig.metricsPropertiesPath)
        .ifPresent(x -> joiner.add("--conf spark.metrics.conf=" + x));
    Optional.ofNullable(distributedConfig.extraClassPath)
        .ifPresent(x -> joiner.add("--conf \"spark.driver.extraClassPath=" + x + "\""));
    Optional.ofNullable(distributedConfig.driverJavaOptions)
        .ifPresent(x -> joiner.add("--driver-java-options \"" + x + "\""));
    Optional.ofNullable(distributedConfig.yarnQueue).ifPresent(x -> joiner.add("--queue " + x));

    if (sparkSettings.getParallelism() < 1) {
      throw new IllegalArgumentException("sparkParallelism can't be 0");
    }

    joiner
        .add("--name=" + sparkAppName)
        .add("--conf spark.default.parallelism=" + sparkSettings.getParallelism())
        .add("--conf spark.executor.memoryOverhead=" + sparkConfig.memoryOverhead)
        .add("--conf spark.dynamicAllocation.enabled=false")
        .add("--conf spark.yarn.am.waitTime=360s")
        .add("--class " + Objects.requireNonNull(distributedConfig.mainClass))
        .add("--master yarn")
        .add("--deploy-mode " + Objects.requireNonNull(distributedConfig.deployMode))
        .add("--executor-memory " + Objects.requireNonNull(sparkSettings.getExecutorMemory()))
        .add("--executor-cores " + sparkConfig.executorCores)
        .add("--num-executors " + sparkSettings.getExecutorNumbers())
        .add("--driver-memory " + sparkConfig.driverMemory)
        .add(Objects.requireNonNull(distributedConfig.jarPath));

    return buildProcessCommon(joiner);
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline
   * options
   */
  private ProcessBuilder buildProcessCommon(StringJoiner command) {

    beamConfigFn.accept(command);

    // Adds user name to run a command if it is necessary
    StringJoiner joiner = new StringJoiner(DELIMITER);
    Optional.ofNullable(distributedConfig.otherUser).ifPresent(x -> joiner.add("sudo -u " + x));
    joiner.merge(command);

    // The result
    String result = joiner.toString();
    log.info("Command - {}", result);

    ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", result);

    builder.redirectError(new File("/dev/null"));
    builder.redirectOutput(new File("/dev/null"));

    return builder;
  }
}
