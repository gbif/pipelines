package org.gbif.pipelines.util;

import static org.gbif.pipelines.coordinator.PostprocessValidation.getValueByKey;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.ThreadContext;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.airflow.AirflowSparkLauncher;
import org.gbif.pipelines.airflow.AppName;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.jetbrains.annotations.NotNull;

@Slf4j(topic = "Pipeline")
public class DistributedUtil {

  public static void runPipeline(
      PipelinesConfig pipelinesConfig,
      PipelineBasedMessage message,
      String jobName,
      FileSystem fileSystem,
      String dagName,
      StepType stepType)
      throws Exception {
    runPipeline(pipelinesConfig, message, jobName, fileSystem, dagName, stepType, List.of());
  }

  public static void runPipeline(
      PipelinesConfig pipelinesConfig,
      PipelineBasedMessage message,
      String jobName,
      FileSystem fileSystem,
      String dagName,
      StepType stepType,
      List<String> extraArgs)
      throws Exception {

    ThreadContext.put("datasetKey", message.getDatasetUuid().toString());
    long start = System.currentTimeMillis();
    Long recordsNumber = getRecordsNumber(pipelinesConfig, message, fileSystem);
    log.info("Starting distributed {}, records count {}", jobName, recordsNumber);

    // App name
    String sparkAppName = AppName.get(stepType, message.getDatasetUuid(), message.getAttempt());

    // create the airflow conf
    SparkConfUtil.Conf conf =
        SparkConfUtil.createConf(
            pipelinesConfig,
            message.getDatasetUuid().toString(),
            message.getAttempt(),
            sparkAppName,
            recordsNumber,
            extraArgs);

    validateSparkConf(conf, sparkAppName);

    log.debug("selected config {}", conf.getDescription());

    // Submit
    AirflowSparkLauncher.builder()
        .airflowConfig(pipelinesConfig.getAirflowConfig())
        .conf(conf)
        .dagName(dagName)
        .sparkAppName(sparkAppName)
        .build()
        .submitAwaitVoid();

    ThreadContext.put("datasetKey", message.getDatasetUuid().toString());
    log.info(timeAndRecPerSecond(jobName, start, recordsNumber));
  }

  private static void validateSparkConf(SparkConfUtil.Conf conf, String sparkAppName) {
    validatePositive("executorInstances", conf.getExecutorInstances(), conf, sparkAppName);
    validatePositive("driverCores", conf.getDriverCores(), conf, sparkAppName);
    validatePositive("executorCores", conf.getExecutorCores(), conf, sparkAppName);
    validatePositive("defaultParallelism", conf.getDefaultParallelism(), conf, sparkAppName);
    validatePositive("numberOfShards", conf.getNumberOfShards(), conf, sparkAppName);

    validateNonBlank(
        "driverMemoryOverheadFactor", conf.getDriverMemoryOverheadFactor(), conf, sparkAppName);
    validateNonBlank(
        "executorMemoryOverheadFactor", conf.getExecutorMemoryOverheadFactor(), conf, sparkAppName);

    validateNonBlank("driverMinCpu", conf.getDriverMinCpu(), conf, sparkAppName);
    validateNonBlank("driverMaxCpu", conf.getDriverMaxCpu(), conf, sparkAppName);
    validateNonBlank("driverLimitMemory", conf.getDriverLimitMemory(), conf, sparkAppName);

    validateNonBlank("executorMinCpu", conf.getExecutorMinCpu(), conf, sparkAppName);
    validateNonBlank("executorMaxCpu", conf.getExecutorMaxCpu(), conf, sparkAppName);
    validateNonBlank("executorLimitMemory", conf.getExecutorLimitMemory(), conf, sparkAppName);

    if (conf.getArgs() == null || conf.getArgs().isEmpty()) {
      throw invalidConf("args must not be null/empty", conf, sparkAppName);
    }
  }

  private static void validatePositive(
      String fieldName, int value, SparkConfUtil.Conf conf, String sparkAppName) {
    if (value <= 0) {
      throw invalidConf(fieldName + " must be > 0, but was " + value, conf, sparkAppName);
    }
  }

  private static void validateNonBlank(
      String fieldName, String value, SparkConfUtil.Conf conf, String sparkAppName) {
    if (value == null || value.trim().isEmpty()) {
      throw invalidConf(fieldName + " must not be null/blank", conf, sparkAppName);
    }
  }

  private static IllegalStateException invalidConf(
      String reason, SparkConfUtil.Conf conf, String sparkAppName) {
    return new IllegalStateException(
        String.format(
            "Invalid Spark config for %s: %s. Config: %s",
            sparkAppName, reason, conf.getDescription()));
  }

  public static String timeAndRecPerSecond(String jobName, long start, long recordsNumber) {
    long end = System.currentTimeMillis();
    Duration d = Duration.between(Instant.ofEpochMilli(start), Instant.ofEpochMilli(end));

    long hours = d.toHours();
    long minutes = d.toMinutesPart();
    long seconds = d.toSecondsPart();
    long millis = d.toMillis() % 1000;

    double secs = Math.max(d.toMillis() / 1000.0, 0.000001);
    double recPerSec = recordsNumber / secs;

    return String.format(
        "Finished %s in %02dh %02dm %02ds %03dms. Rec/s: %6.2f, Total records: %,d",
        jobName, hours, minutes, seconds, millis, recPerSec, recordsNumber);
  }

  @NotNull
  public static Long getRecordsNumber(
      PipelinesConfig pipelinesConfig, PipelineBasedMessage message, FileSystem fileSystem)
      throws IOException {

    String metaPath =
        String.join(
            "/",
            pipelinesConfig.getOutputPath(),
            message.getDatasetUuid().toString(),
            message.getAttempt().toString(),
            "archive-to-verbatim.yml");

    log.debug("Reading path for record number {}", metaPath);

    // archiveToOccurrenceCount
    long recordCount =
        Long.parseLong(
            getValueByKey(
                    fileSystem, metaPath, PipelinesVariables.Metrics.ARCHIVE_TO_LARGEST_FILE_COUNT)
                .orElse("0"));

    if (recordCount > 0) {
      return recordCount;
    }

    // fall back to older counts
    long occRecordCount =
        Long.parseLong(
            getValueByKey(fileSystem, metaPath, PipelinesVariables.Metrics.ARCHIVE_TO_OCC_COUNT)
                .orElse("0"));

    long erRecordCount =
        Long.parseLong(
            getValueByKey(fileSystem, metaPath, PipelinesVariables.Metrics.ARCHIVE_TO_ER_COUNT)
                .orElse("0"));

    return Math.max(occRecordCount, erRecordCount);
  }
}
