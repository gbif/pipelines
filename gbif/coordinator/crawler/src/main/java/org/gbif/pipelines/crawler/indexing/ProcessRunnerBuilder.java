package org.gbif.pipelines.crawler.indexing;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;

/** Class to build an instance of ProcessBuilder for direct or spark command */
@SuppressWarnings("all")
@Slf4j
@Builder
final class ProcessRunnerBuilder {

  private static final String DELIMITER = " ";

  @NonNull private IndexingConfiguration config;
  @NonNull private PipelinesInterpretedMessage message;
  private String esAlias;
  @NonNull private String esIndexName;
  private Integer esShardsNumber;
  private int sparkParallelism;
  private int sparkExecutorNumbers;
  private String sparkExecutorMemory;
  private String sparkEventLogDir;

  ProcessBuilder get() {
    if (StepRunner.DISTRIBUTED.name().equals(config.processRunner)) {
      return buildSpark();
    }
    throw new IllegalArgumentException("Wrong runner type - " + config.processRunner);
  }

  public String[] buildOptions() {
    return buildCommonOptions(new StringJoiner(DELIMITER)).split(DELIMITER);
  }

  /** Builds ProcessBuilder to process spark command */
  private ProcessBuilder buildSpark() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("spark2-submit");

    Optional.ofNullable(config.distributedConfig.metricsPropertiesPath)
        .ifPresent(x -> joiner.add("--conf spark.metrics.conf=" + x));
    Optional.ofNullable(config.distributedConfig.extraClassPath)
        .ifPresent(x -> joiner.add("--conf \"spark.driver.extraClassPath=" + x + "\""));
    Optional.ofNullable(config.distributedConfig.driverJavaOptions)
        .ifPresent(x -> joiner.add("--driver-java-options \"" + x + "\""));
    Optional.ofNullable(config.distributedConfig.yarnQueue)
        .ifPresent(x -> joiner.add("--queue " + x));

    if (sparkParallelism < 1) {
      throw new IllegalArgumentException("sparkParallelism can't be 0");
    }

    joiner
        .add("--conf spark.default.parallelism=" + sparkParallelism)
        .add("--conf spark.executor.memoryOverhead=" + config.sparkConfig.memoryOverhead)
        .add("--conf spark.dynamicAllocation.enabled=false")
        .add("--conf spark.yarn.am.waitTime=360s")
        .add("--class " + Objects.requireNonNull(config.distributedConfig.mainClass))
        .add("--master yarn")
        .add("--deploy-mode " + Objects.requireNonNull(config.distributedConfig.deployMode))
        .add("--executor-memory " + Objects.requireNonNull(sparkExecutorMemory))
        .add("--executor-cores " + config.sparkConfig.executorCores)
        .add("--num-executors " + sparkExecutorNumbers)
        .add("--driver-memory " + config.sparkConfig.driverMemory)
        .add(Objects.requireNonNull(config.distributedConfig.jarPath));

    return buildCommon(joiner);
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline
   * options
   */
  private String buildCommonOptions(StringJoiner command) {

    String esHosts = String.join(",", config.esConfig.hosts);

    // Common properties
    command
        .add("--datasetId=" + Objects.requireNonNull(message.getDatasetUuid()))
        .add("--attempt=" + message.getAttempt())
        .add("--runner=SparkRunner")
        .add("--inputPath=" + Objects.requireNonNull(config.stepConfig.repositoryPath))
        .add("--targetPath=" + Objects.requireNonNull(config.stepConfig.repositoryPath))
        .add("--metaFileName=" + Objects.requireNonNull(config.metaFileName))
        .add("--hdfsSiteConfig=" + Objects.requireNonNull(config.stepConfig.hdfsSiteConfig))
        .add("--coreSiteConfig=" + Objects.requireNonNull(config.stepConfig.coreSiteConfig))
        .add("--esHosts=" + Objects.requireNonNull(esHosts))
        .add("--properties=" + Objects.requireNonNull(config.pipelinesConfig))
        .add("--esIndexName=" + Objects.requireNonNull(esIndexName));

    Optional.ofNullable(esAlias).ifPresent(x -> command.add("--esAlias=" + x));
    Optional.ofNullable(config.esConfig.maxBatchSizeBytes)
        .ifPresent(x -> command.add("--esMaxBatchSizeBytes=" + x));
    Optional.ofNullable(config.esConfig.maxBatchSize)
        .ifPresent(x -> command.add("--esMaxBatchSize=" + x));
    Optional.ofNullable(config.esConfig.schemaPath)
        .ifPresent(x -> command.add("--esSchemaPath=" + x));
    Optional.ofNullable(config.indexConfig.refreshInterval)
        .ifPresent(x -> command.add("--indexRefreshInterval=" + x));
    Optional.ofNullable(esShardsNumber).ifPresent(x -> command.add("--indexNumberShards=" + x));
    Optional.ofNullable(config.indexConfig.numberReplicas)
        .ifPresent(x -> command.add("--indexNumberReplicas=" + x));

    if (config.useBeamDeprecatedRead) {
      command.add("--experiments=use_deprecated_read");
    }

    return command.toString();
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline
   * options
   */
  private ProcessBuilder buildCommon(StringJoiner command) {
    buildCommonOptions(command);

    // Adds user name to run a command if it is necessary
    StringJoiner joiner = new StringJoiner(DELIMITER);
    Optional.ofNullable(config.distributedConfig.otherUser)
        .ifPresent(x -> joiner.add("sudo -u " + x));
    joiner.merge(command);

    // The result
    String result = joiner.toString();
    log.info("Command - {}", result);

    ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", result);

    // The command side outputs
    builder.redirectError(new File("/dev/null"));
    builder.redirectOutput(new File("/dev/null"));

    return builder;
  }
}
