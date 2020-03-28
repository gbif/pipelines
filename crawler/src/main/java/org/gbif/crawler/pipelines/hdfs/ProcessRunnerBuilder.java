package org.gbif.crawler.pipelines.hdfs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.BiFunction;

import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to build an instance of ProcessBuilder for direct or spark command
 */
final class ProcessRunnerBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessRunnerBuilder.class);

  private static final String DELIMITER = " ";

  private HdfsViewConfiguration config;
  private PipelinesInterpretedMessage message;
  private int sparkParallelism;
  private int sparkExecutorNumbers;
  private String sparkExecutorMemory;
  private String sparkEventLogDir;
  private int numberOfShards;

  ProcessRunnerBuilder config(HdfsViewConfiguration config) {
    this.config = Objects.requireNonNull(config);
    return this;
  }

  ProcessRunnerBuilder message(PipelinesInterpretedMessage message) {
    this.message = Objects.requireNonNull(message);
    return this;
  }

  ProcessRunnerBuilder sparkParallelism(int sparkParallelism) {
    this.sparkParallelism = sparkParallelism;
    return this;
  }

  ProcessRunnerBuilder sparkExecutorNumbers(int sparkExecutorNumbers) {
    this.sparkExecutorNumbers = sparkExecutorNumbers;
    return this;
  }

  ProcessRunnerBuilder sparkExecutorMemory(String sparkExecutorMemory) {
    this.sparkExecutorMemory = sparkExecutorMemory;
    return this;
  }

  ProcessRunnerBuilder sparkEventLogDir(String sparkEventLogDir) {
    this.sparkEventLogDir = sparkEventLogDir;
    return this;
  }

  ProcessRunnerBuilder numberOfShards(int numberOfShards) {
    this.numberOfShards = numberOfShards;
    return this;
  }

  static ProcessRunnerBuilder create() {
    return new ProcessRunnerBuilder();
  }

  ProcessBuilder build() {
    if (StepRunner.STANDALONE.name().equals(config.processRunner)) {
      return buildDirect();
    }
    if (StepRunner.DISTRIBUTED.name().equals(config.processRunner)) {
      return buildSpark();
    }
    throw new IllegalArgumentException("Wrong runner type - " + config.processRunner);
  }

  public String[] buildOptions() {
    return buildCommonOptions(new StringJoiner(DELIMITER)).split(DELIMITER);
  }

  /**
   * Builds ProcessBuilder to process direct command
   */
  private ProcessBuilder buildDirect() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("java")
        .add("-XX:+UseG1GC")
        .add("-Xms" + Objects.requireNonNull(config.standaloneStackSize))
        .add("-Xmx" + Objects.requireNonNull(config.standaloneHeapSize))
        .add(Objects.requireNonNull(config.driverJavaOptions))
        .add("-cp")
        .add(Objects.requireNonNull(config.standaloneJarPath))
        .add(Objects.requireNonNull(config.standaloneMainClass))
        .add("--pipelineStep=INTERPRETED_TO_HDFS");

    Optional.ofNullable(sparkEventLogDir).ifPresent(sparkEventLogDir -> joiner.add("--conf spark.eventLog.enabled=true")
                                                                              .add("--conf spark.eventLog.dir=" +  sparkEventLogDir));

    return buildCommon(joiner);
  }

  /**
   * Builds ProcessBuilder to process spark command
   */
  private ProcessBuilder buildSpark() {
    StringJoiner joiner = new StringJoiner(DELIMITER).add("spark2-submit");

    Optional.ofNullable(config.metricsPropertiesPath).ifPresent(x -> joiner.add("--conf spark.metrics.conf=" + x));
    Optional.ofNullable(config.extraClassPath)
        .ifPresent(x -> joiner.add("--conf \"spark.driver.extraClassPath=" + x + "\""));
    Optional.ofNullable(config.driverJavaOptions).ifPresent(x -> joiner.add("--driver-java-options \"" + x + "\""));
    Optional.ofNullable(config.yarnQueue).ifPresent(x -> joiner.add("--queue " + x));

    if (sparkParallelism < 1) {
      throw new IllegalArgumentException("sparkParallelism can't be 0");
    }

    joiner.add("--conf spark.default.parallelism=" + sparkParallelism)
        .add("--conf spark.executor.memoryOverhead=" + config.sparkMemoryOverhead)
        .add("--conf spark.dynamicAllocation.enabled=false")
        .add("--class " + Objects.requireNonNull(config.distributedMainClass))
        .add("--master yarn")
        .add("--deploy-mode " + Objects.requireNonNull(config.deployMode))
        .add("--executor-memory " + Objects.requireNonNull(sparkExecutorMemory))
        .add("--executor-cores " + config.sparkExecutorCores)
        .add("--num-executors " + sparkExecutorNumbers)
        .add("--driver-memory " + config.sparkDriverMemory)
        .add(Objects.requireNonNull(config.distributedJarPath));

    return buildCommon(joiner);
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline options
   */
  private String buildCommonOptions(StringJoiner command) {
    // Common properties
    command.add("--datasetId=" + Objects.requireNonNull(message.getDatasetUuid()))
        .add("--attempt=" + message.getAttempt())
        .add("--runner=SparkRunner")
        .add("--metaFileName=" + Objects.requireNonNull(config.metaFileName))
        .add("--inputPath=" + Objects.requireNonNull(config.repositoryPath))
        .add("--targetPath=" + Objects.requireNonNull(config.repositoryTargetPath))
        .add("--hdfsSiteConfig=" + Objects.requireNonNull(config.hdfsSiteConfig))
        .add("--coreSiteConfig=" + Objects.requireNonNull(config.coreSiteConfig))
        .add("--numberOfShards=" + numberOfShards)
        .add("--properties=" + Objects.requireNonNull(config.pipelinesConfig));

    return command.toString();
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline options
   */
  private ProcessBuilder buildCommon(StringJoiner command) {

    buildCommonOptions(command);

    // Adds user name to run a command if it is necessary
    StringJoiner joiner = new StringJoiner(DELIMITER);
    Optional.ofNullable(config.otherUser).ifPresent(x -> joiner.add("sudo -u " + x));
    joiner.merge(command);

    // The result
    String result = joiner.toString();
    LOG.info("Command - {}", result);

    ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", result);

    BiFunction<String, String, File> createDirFn = (String type, String path) -> {
      try {
        Files.createDirectories(Paths.get(path));
        File file = new File(path + message.getDatasetUuid() + "_" + message.getAttempt() + "_idx_" + type + ".log");
        LOG.info("{} file - {}", type, file);
        return file;
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    };

    // The command side outputs
    if (config.processErrorDirectory != null) {
      builder.redirectError(createDirFn.apply("err", config.processErrorDirectory));
    } else {
      builder.redirectError(new File("/dev/null"));
    }

    if (config.processOutputDirectory != null) {
      builder.redirectOutput(createDirFn.apply("out", config.processOutputDirectory));
    } else {
      builder.redirectOutput(new File("/dev/null"));
    }

    return builder;
  }

}
