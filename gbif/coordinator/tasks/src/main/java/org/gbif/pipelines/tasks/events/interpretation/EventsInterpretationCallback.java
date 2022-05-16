package org.gbif.pipelines.tasks.events.interpretation;

import java.io.IOException;
import java.util.Collections;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.events.interpretation.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;

/** Callback which is called when the {@link PipelinesEventsMessage} is received. */
@Slf4j
public class EventsInterpretationCallback extends AbstractMessageCallback<PipelinesEventsMessage>
    implements StepHandler<PipelinesEventsMessage, PipelinesEventsInterpretedMessage> {

  private final EventsInterpretationConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final HdfsConfigs hdfsConfigs;

  public EventsInterpretationCallback(
    EventsInterpretationConfiguration config, MessagePublisher publisher, CuratorFramework curator
  ) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    hdfsConfigs = HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
  }

  @Override
  public void handleMessage(PipelinesEventsMessage message) {
    StepType type = StepType.EVENTS_VERBATIM_TO_INTERPRETED;
    PipelinesCallback.<PipelinesEventsMessage, PipelinesEventsInterpretedMessage>builder()
        .config(config)
        .curator(curator)
        .stepType(type)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public boolean isMessageCorrect(PipelinesEventsMessage message) {
    return message.getDatasetType() == DatasetType.SAMPLING_EVENT
        && message.getNumberOfEventRecords() > 0;
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index
   * pipeline
   */
  @Override
  public Runnable createRunnable(PipelinesEventsMessage message) {
    return () -> {
      try {
        String datasetId = message.getDatasetUuid().toString();
        String attempt = Integer.toString(message.getAttempt());

        String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
        String path =
            String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, verbatim);

        ProcessRunnerBuilderBuilder builder =
            ProcessRunnerBuilder.builder().config(config).inputPath(path).message(message);

        log.info("Start the process. Message - {}", message);
        runDistributed(builder, message);

        log.info("Deleting old attempts directories");
        String pathToDelete = String.join("/", config.stepConfig.repositoryPath, datasetId);
        HdfsUtils.deleteSubFolders(
            hdfsConfigs,
            pathToDelete,
            config.deleteAfterDays,
            Collections.singleton(attempt));
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  @Override
  public PipelinesEventsInterpretedMessage createOutgoingMessage(PipelinesEventsMessage message) {
    boolean repeatAttempt = pathExists(message);
    return new PipelinesEventsInterpretedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        message.getNumberOfOccurrenceRecords(),
        message.getNumberOfEventRecords(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getEndpointType(),
        message.getInterpretTypes(),
        repeatAttempt,
        message.getRunner());
  }

  private void runDistributed(ProcessRunnerBuilderBuilder builder, PipelinesEventsMessage message)
      throws IOException, InterruptedException {
    int sparkExecutorNumbers = computeSparkExecutorNumbers(message.getNumberOfEventRecords());

    builder
        .sparkParallelism(computeSparkParallelism(sparkExecutorNumbers))
        .sparkExecutorMemory(computeSparkExecutorMemory(sparkExecutorNumbers))
        .sparkExecutorNumbers(sparkExecutorNumbers);

    // Assembles a terminal java process and runs it
    int exitValue = builder.build().get().start().waitFor();

    if (exitValue != 0) {
      throw new IllegalStateException("Process has been finished with exit value - " + exitValue);
    } else {
      log.info("Process has been finished with exit value - {}", exitValue);
    }
  }

  /** Checks if the directory exists */
  @SneakyThrows
  private boolean pathExists(PipelinesEventsMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String path =
        String.join(
            "/",
            config.stepConfig.repositoryPath,
            datasetId,
            attempt,
            Interpretation.DIRECTORY_NAME);

    return HdfsUtils.exists(hdfsConfigs, path);
  }

  /**
   * Compute the number of thread for spark.default.parallelism, top limit is
   * config.sparkParallelismMax Remember YARN will create the same number of files
   */
  private int computeSparkParallelism(int executorNumbers) {
    int count = executorNumbers * config.sparkConfig.executorCores * 2;

    if (count < config.sparkConfig.parallelismMin) {
      return config.sparkConfig.parallelismMin;
    }
    if (count > config.sparkConfig.parallelismMax) {
      return config.sparkConfig.parallelismMax;
    }
    return count;
  }

  /**
   * Computes the memory for executor in Gb, where min is config.sparkExecutorMemoryGbMin and max is
   * config.sparkExecutorMemoryGbMax
   */
  private String computeSparkExecutorMemory(int sparkExecutorNumbers) {

    if (sparkExecutorNumbers < config.sparkConfig.executorMemoryGbMin) {
      return config.sparkConfig.executorMemoryGbMin + "G";
    }
    if (sparkExecutorNumbers > config.sparkConfig.executorMemoryGbMax) {
      return config.sparkConfig.executorMemoryGbMax + "G";
    }
    return sparkExecutorNumbers + "G";
  }

  /**
   * Computes the numbers of executors, where min is config.sparkConfig.executorNumbersMin and max
   * is config.sparkConfig.executorNumbersMax
   */
  private int computeSparkExecutorNumbers(long recordsNumber) {
    int sparkExecutorNumbers =
        (int)
            Math.ceil(
                (double) recordsNumber
                    / (config.sparkConfig.executorCores * config.sparkConfig.recordsPerThread));
    if (sparkExecutorNumbers < config.sparkConfig.executorNumbersMin) {
      return config.sparkConfig.executorNumbersMin;
    }
    if (sparkExecutorNumbers > config.sparkConfig.executorNumbersMax) {
      return config.sparkConfig.executorNumbersMax;
    }
    return sparkExecutorNumbers;
  }
}
