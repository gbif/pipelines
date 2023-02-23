package org.gbif.pipelines.common.hdfs;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretationMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.interpretation.SparkSettings;
import org.gbif.pipelines.common.process.BeamSettings;
import org.gbif.pipelines.common.process.ProcessRunnerBuilder;
import org.gbif.pipelines.common.process.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.ingest.java.pipelines.HdfsViewPipeline;
import org.gbif.pipelines.tasks.occurrences.interpretation.InterpreterConfiguration;

/** Callback which is called when an instance {@link PipelinesInterpretationMessage} is received. */
@Slf4j
@AllArgsConstructor(staticName = "create")
public class CommonHdfsViewCallback {

  private final HdfsViewConfiguration config;
  private final ExecutorService executor;

  /** Main message processing logic, creates a terminal java process, which runs */
  public Runnable createRunnable(PipelinesInterpretationMessage message) {
    return () -> {
      try {

        // If there is one step only like metadata, we have to run pipelines steps
        message.setInterpretTypes(swapInterpretTypes(message.getInterpretTypes()));

        int fileShards = computeNumberOfShards(message);

        ProcessRunnerBuilderBuilder builder =
            ProcessRunnerBuilder.builder()
                .distributedConfig(config.distributedConfig)
                .sparkConfig(config.sparkConfig)
                .sparkAppName(
                    config.stepType + "_" + message.getDatasetUuid() + "_" + message.getAttempt())
                .beamConfigFn(BeamSettings.occurrenceHdfsView(config, message, fileShards));

        Predicate<StepRunner> runnerPr = sr -> config.processRunner.equalsIgnoreCase(sr.name());

        log.info("Start the process. Message - {}", message);
        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message, builder);
        } else if (runnerPr.test(StepRunner.STANDALONE)) {
          runLocal(builder);
        }
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in
   * service config {@link HdfsViewConfiguration#processRunner}
   */
  public boolean isMessageCorrect(PipelinesInterpretationMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message);
    }
    boolean isCorrectProcess = config.processRunner.equals(message.getRunner());
    if (!isCorrectProcess) {
      log.info("Skipping, because expected step is incorrect");
    }
    return isCorrectProcess;
  }

  private void runLocal(ProcessRunnerBuilderBuilder builder) {
    HdfsViewPipeline.run(builder.build().buildOptions(), executor);
  }

  private void runDistributed(
      PipelinesInterpretationMessage message, ProcessRunnerBuilderBuilder builder)
      throws IOException, InterruptedException {

    long recordsNumber = getRecordNumber(message);
    log.info("Calculate job's settings based on {} records", recordsNumber);
    SparkSettings sparkSettings = SparkSettings.create(config.sparkConfig, recordsNumber);

    builder.sparkSettings(sparkSettings);

    // Assembles a terminal java process and runs it
    int exitValue = builder.build().get().start().waitFor();

    if (exitValue != 0) {
      throw new IllegalStateException("Process has been finished with exit value - " + exitValue);
    } else {
      log.info("Process has been finished with exit value - {}", exitValue);
    }
  }

  /**
   * Reads number of records from an archive-to-avro metadata file, verbatim-to-interpreted contains
   * attempted records count, which is not accurate enough
   */
  private long getRecordNumber(PipelinesInterpretationMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new InterpreterConfiguration().metaFileName;
    String metaPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);

    Long messageNumber = null;
    if (message instanceof PipelinesInterpretedMessage) {
      messageNumber = ((PipelinesInterpretedMessage) message).getNumberOfRecords();
    } else if (message instanceof PipelinesEventsInterpretedMessage) {
      messageNumber = ((PipelinesEventsInterpretedMessage) message).getNumberOfEventRecords();
    }

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);

    Optional<Long> fileNumber =
        HdfsUtils.getLongByKey(
            hdfsConfigs, metaPath, Metrics.BASIC_RECORDS_COUNT + Metrics.ATTEMPTED);
    if (!fileNumber.isPresent()) {
      fileNumber =
          HdfsUtils.getLongByKey(
              hdfsConfigs, metaPath, Metrics.UNIQUE_GBIF_IDS_COUNT + Metrics.ATTEMPTED);
    }

    if (messageNumber == null && !fileNumber.isPresent()) {
      throw new IllegalArgumentException(
          "Please check archive-to-avro metadata yaml file or message records number, recordsNumber can't be null or empty!");
    }

    if (messageNumber == null) {
      return fileNumber.get();
    }

    if (!fileNumber.isPresent() || messageNumber > fileNumber.get()) {
      return messageNumber;
    }

    return fileNumber.get();
  }

  private int computeNumberOfShards(PipelinesInterpretationMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String dirPath =
        String.join(
            "/",
            config.stepConfig.repositoryPath,
            datasetId,
            attempt,
            config.recordType == RecordType.EVENT
                ? DwcTerm.Event.simpleName().toLowerCase()
                : DwcTerm.Occurrence.simpleName().toLowerCase());
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    long sizeByte = HdfsUtils.getFileSizeByte(hdfsConfigs, dirPath);
    if (sizeByte == -1d) {
      throw new IllegalArgumentException(
          "Please check interpretation source directory! - " + dirPath);
    }
    long sizeExpected = config.hdfsAvroExpectedFileSizeInMb * 1048576L; // 1024 * 1024
    double numberOfShards = (sizeByte * config.hdfsAvroCoefficientRatio / 100f) / sizeExpected;
    double numberOfShardsFloor = Math.floor(numberOfShards);
    numberOfShards =
        numberOfShards - numberOfShardsFloor > 0.5d ? numberOfShardsFloor + 1 : numberOfShardsFloor;
    return numberOfShards <= 0 ? 1 : (int) numberOfShards;
  }

  // If there is one step only like metadata, we have to run the RecordType steps
  private Set<String> swapInterpretTypes(Set<String> interpretTypes) {
    if (interpretTypes.isEmpty()) {
      return Collections.singleton(RecordType.ALL.name());
    }
    if (interpretTypes.size() == 1 && interpretTypes.contains(RecordType.ALL.name())) {
      return Collections.singleton(RecordType.ALL.name());
    }
    if (interpretTypes.size() == 1
        && RecordType.getAllInterpretationAsString().containsAll(interpretTypes)) {
      return Collections.singleton(config.recordType.name());
    }
    return interpretTypes;
  }
}
