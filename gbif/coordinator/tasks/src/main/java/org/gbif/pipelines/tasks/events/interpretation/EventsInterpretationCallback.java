package org.gbif.pipelines.tasks.events.interpretation;

import java.io.IOException;
import java.util.Collections;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.events.interpretation.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;

/** Callback which is called when the {@link PipelinesEventsMessage} is received. */
@Slf4j
@AllArgsConstructor
public class EventsInterpretationCallback extends AbstractMessageCallback<PipelinesEventsMessage>
    implements StepHandler<PipelinesEventsMessage, PipelinesEventsInterpretedMessage> {

  private final EventsInterpretationConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

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
    return message.getDatasetType() == DatasetType.SAMPLING_EVENT;
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index
   * pipeline
   */
  @Override
  public Runnable createRunnable(PipelinesEventsMessage message) {
    return () -> {
      try {
        ProcessRunnerBuilderBuilder builder =
            ProcessRunnerBuilder.builder().config(config).message(message);

        log.info("Start the process. Message - {}", message);
        runDistributed(builder);

        log.info("Deleting old attempts directories");
        String datasetId = message.getDatasetUuid().toString();
        String attempt = Integer.toString(message.getAttempt());
        String pathToDelete = String.join("/", config.stepConfig.repositoryPath, datasetId);
        HdfsUtils.deleteSubFolders(
            config.stepConfig.hdfsSiteConfig,
            config.stepConfig.coreSiteConfig,
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
    // TODO: get number of records from event metrics?

    boolean repeatAttempt = pathExists(message);

    return new PipelinesEventsInterpretedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        message.getNumberOfRecords(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getEndpointType(),
        message.getValidationResult(),
        repeatAttempt,
        message.isValidator());
  }

  private void runDistributed(ProcessRunnerBuilderBuilder builder)
      throws IOException, InterruptedException {
    builder
        .sparkParallelism(computeSparkParallelism())
        .sparkExecutorMemory(computeSparkExecutorMemory(config.sparkConfig.executorCores))
        .sparkExecutorNumbers(config.sparkConfig.executorCores);

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

    return HdfsUtils.exists(
        config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig, path);
  }

  /**
   * Computes the number of thread for spark.default.parallelism, top limit is
   * config.sparkParallelismMax
   */
  private int computeSparkParallelism() {
    // TODO: refine this
    int parallelism = 50;
    if (parallelism < config.sparkConfig.parallelismMin) {
      return config.sparkConfig.parallelismMin;
    }
    if (parallelism > config.sparkConfig.parallelismMax) {
      return config.sparkConfig.parallelismMax;
    }
    return parallelism;
  }

  /**
   * Computes the memory for executor in Gb, where min is config.sparkConfig.executorMemoryGbMin and
   * max is config.sparkConfig.executorMemoryGbMax
   */
  private String computeSparkExecutorMemory(int sparkExecutorNumbers) {
    // TODO: refine this
    int size = sparkExecutorNumbers + 2;

    if (size < config.sparkConfig.executorMemoryGbMin) {
      return config.sparkConfig.executorMemoryGbMin + "G";
    }
    if (size > config.sparkConfig.executorMemoryGbMax) {
      return config.sparkConfig.executorMemoryGbMax + "G";
    }
    return size + "G";
  }
}
