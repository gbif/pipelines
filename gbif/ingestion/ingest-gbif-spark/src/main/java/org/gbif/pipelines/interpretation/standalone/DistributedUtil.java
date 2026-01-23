package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.standalone.PostprocessValidation.getValueByKey;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.airflow.AirflowConfFactory;
import org.gbif.pipelines.airflow.AirflowSparkLauncher;
import org.gbif.pipelines.airflow.AppName;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;

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

    MDC.put("datasetKey", message.getDatasetUuid().toString());
    long start = System.currentTimeMillis();
    Long recordsNumber = getRecordsNumber(pipelinesConfig, message, fileSystem);
    log.info("Starting distributed {}, records count {}", jobName, recordsNumber);

    // App name
    String sparkAppName = AppName.get(stepType, message.getDatasetUuid(), message.getAttempt());

    // create the airflow conf
    AirflowConfFactory.Conf conf =
        AirflowConfFactory.createConf(
            pipelinesConfig,
            message.getDatasetUuid().toString(),
            message.getAttempt(),
            sparkAppName,
            recordsNumber,
            extraArgs);

    log.debug("selected config {}", conf.getDescription());

    // Submit
    AirflowSparkLauncher.builder()
        .airflowConfig(pipelinesConfig.getAirflowConfig())
        .conf(conf)
        .dagName(dagName)
        .sparkAppName(sparkAppName)
        .build()
        .submitAwaitVoid();

    MDC.put("datasetKey", message.getDatasetUuid().toString());
    log.info(timeAndRecPerSecond(jobName, start, recordsNumber));
    MDC.remove("datasetKey");
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
    long occCount =
        Long.parseLong(
            getValueByKey(fileSystem, metaPath, PipelinesVariables.Metrics.ARCHIVE_TO_OCC_COUNT)
                .orElse("0"));

    long erCount =
        Long.parseLong(
            getValueByKey(fileSystem, metaPath, PipelinesVariables.Metrics.ARCHIVE_TO_ER_COUNT)
                .orElse("0"));
    return Math.max(occCount, erCount);
  }
}
