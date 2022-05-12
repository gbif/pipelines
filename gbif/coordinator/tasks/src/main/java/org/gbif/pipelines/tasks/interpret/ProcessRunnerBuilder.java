package org.gbif.pipelines.tasks.interpret;

import static org.gbif.pipelines.common.utils.ValidatorPredicate.isValidator;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;

/** Class to build an instance of ProcessBuilder for direct or spark command */
@SuppressWarnings("all")
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
  private String defaultDateFormat;
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
        .add("--name=" + getAppName())
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

    Optional.ofNullable(defaultDateFormat).ifPresent(x -> command.add("--defaultDateFormat=" + x));

    if (isValidator(message.getPipelineSteps(), config.validatorOnly)) {
      command.add("--useMetadataWsCalls=false");
    }

    if (config.skipGbifIds) {
      command
          .add("--tripletValid=false")
          .add("--occurrenceIdValid=false")
          .add("--useExtendedRecordId=true");
    } else {
      Optional.ofNullable(message.getValidationResult())
          .ifPresent(
              vr ->
                  command
                      .add("--tripletValid=" + vr.isTripletValid())
                      .add("--occurrenceIdValid=" + vr.isOccurrenceIdValid()));

      Optional.ofNullable(message.getValidationResult())
          .flatMap(vr -> Optional.ofNullable(vr.isUseExtendedRecordId()))
          .ifPresent(x -> command.add("--useExtendedRecordId=" + x));
    }

    if (config.useBeamDeprecatedRead) {
      command.add("--experiments=use_deprecated_read");
    }

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
    Optional.ofNullable(config.distributedConfig.otherUser)
        .ifPresent(x -> joiner.add("sudo -u " + x));
    joiner.merge(command);

    // The result
    String result = joiner.toString();
    log.info("Command - {}", result);

    ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", result);

    builder.redirectError(new File("/dev/null"));
    builder.redirectOutput(new File("/dev/null"));

    return builder;
  }

  private String getAppName() {
    String type = StepType.VERBATIM_TO_INTERPRETED.name();
    if (message.getPipelineSteps().contains(StepType.VALIDATOR_VERBATIM_TO_INTERPRETED.name())) {
      type = StepType.VALIDATOR_VERBATIM_TO_INTERPRETED.name();
    }
    return type + "_" + message.getDatasetUuid() + "_" + message.getAttempt();
  }
}
