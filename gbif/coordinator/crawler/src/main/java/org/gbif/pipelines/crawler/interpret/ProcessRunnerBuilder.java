package org.gbif.pipelines.crawler.interpret;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.BiFunction;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;

/** Class to build an instance of ProcessBuilder for direct or spark command */
@Slf4j
@Builder
final class ProcessRunnerBuilder {

  private static final String DELIMITER = " ";

  private InterpreterConfiguration config;
  @NonNull private PipelinesVerbatimMessage message;
  private int sparkParallelism;
  private int sparkExecutorNumbers;
  private String sparkExecutorMemory;
  private String sparkEventLogDir;
  @NonNull private String inputPath;

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

    Optional.ofNullable(config.metricsPropertiesPath)
        .ifPresent(x -> joiner.add("--conf spark.metrics.conf=" + x));
    Optional.ofNullable(config.extraClassPath)
        .ifPresent(x -> joiner.add("--conf \"spark.driver.extraClassPath=" + x + "\""));
    Optional.ofNullable(config.driverJavaOptions)
        .ifPresent(x -> joiner.add("--driver-java-options \"" + x + "\""));
    Optional.ofNullable(config.yarnQueue).ifPresent(x -> joiner.add("--queue " + x));

    if (sparkParallelism < 1) {
      throw new IllegalArgumentException("sparkParallelism can't be 0");
    }

    joiner
        .add("--conf spark.default.parallelism=" + sparkParallelism)
        .add("--conf spark.executor.memoryOverhead=" + config.sparkMemoryOverhead)
        .add("--conf spark.dynamicAllocation.enabled=false")
        .add("--conf spark.yarn.am.waitTime=360s")
        .add("--class " + Objects.requireNonNull(config.distributedMainClass))
        .add("--master yarn")
        .add("--deploy-mode " + Objects.requireNonNull(config.deployMode))
        .add("--executor-memory " + Objects.requireNonNull(sparkExecutorMemory))
        .add("--executor-cores " + config.sparkExecutorCores)
        .add("--num-executors " + sparkExecutorNumbers)
        .add("--driver-memory " + config.sparkDriverMemory)
        .add(Objects.requireNonNull(config.distributedJarPath));

    return buildProcessCommon(joiner);
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline
   * options
   */
  private String buildCommonOptions(StringJoiner command) {

    String interpretationTypes = String.join(",", message.getInterpretTypes());

    // Common properties
    command
        .add("--datasetId=" + Objects.requireNonNull(message.getDatasetUuid()))
        .add("--attempt=" + message.getAttempt())
        .add("--interpretationTypes=" + Objects.requireNonNull(interpretationTypes))
        .add("--runner=SparkRunner")
        .add("--targetPath=" + Objects.requireNonNull(config.stepConfig.repositoryPath))
        .add("--metaFileName=" + Objects.requireNonNull(config.metaFileName))
        .add("--inputPath=" + Objects.requireNonNull(inputPath))
        .add("--avroCompressionType=" + Objects.requireNonNull(config.avroConfig.compressionType))
        .add("--avroSyncInterval=" + config.avroConfig.syncInterval)
        .add("--hdfsSiteConfig=" + Objects.requireNonNull(config.stepConfig.hdfsSiteConfig))
        .add("--coreSiteConfig=" + Objects.requireNonNull(config.stepConfig.coreSiteConfig))
        .add("--properties=" + Objects.requireNonNull(config.pipelinesConfig))
        .add("--endPointType=" + Objects.requireNonNull(message.getEndpointType()));

    Optional.ofNullable(message.getValidationResult())
        .ifPresent(
            vr ->
                command
                    .add("--tripletValid=" + vr.isTripletValid())
                    .add("--occurrenceIdValid=" + vr.isOccurrenceIdValid()));

    Optional.ofNullable(message.getValidationResult())
        .flatMap(vr -> Optional.ofNullable(vr.isUseExtendedRecordId()))
        .ifPresent(x -> command.add("--useExtendedRecordId=" + x));

    return command.toString();
  }

  /**
   * Adds common properties to direct or spark process, for running Java pipelines with pipeline
   * options
   */
  private ProcessBuilder buildProcessCommon(StringJoiner command) {

    buildCommonOptions(command);

    // Adds user name to run a command if it is necessary
    StringJoiner joiner = new StringJoiner(DELIMITER);
    Optional.ofNullable(config.otherUser).ifPresent(x -> joiner.add("sudo -u " + x));
    joiner.merge(command);

    // The result
    String result = joiner.toString();
    log.info("Command - {}", result);

    ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", result);

    BiFunction<String, String, File> createDirFn =
        (String type, String path) -> {
          try {
            Files.createDirectories(Paths.get(path));
            File file =
                new File(
                    path
                        + message.getDatasetUuid()
                        + "_"
                        + message.getAttempt()
                        + "_int_"
                        + type
                        + ".log");
            log.info("{} file - {}", type, file);
            return file;
          } catch (IOException ex) {
            throw new IllegalStateException(ex);
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
