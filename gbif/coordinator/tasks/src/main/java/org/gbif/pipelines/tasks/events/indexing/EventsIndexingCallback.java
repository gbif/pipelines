package org.gbif.pipelines.tasks.events.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;

/** Callback which is called when the {@link PipelinesEventsMessage} is received. */
@Slf4j
@AllArgsConstructor
public class EventsIndexingCallback
    extends AbstractMessageCallback<PipelinesEventsInterpretedMessage>
    implements StepHandler<PipelinesEventsInterpretedMessage, PipelinesEventsIndexedMessage> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final EventsIndexingConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

  @Override
  public void handleMessage(PipelinesEventsInterpretedMessage message) {
    StepType type = StepType.EVENTS_INTERPRETED_TO_INDEX;
    PipelinesCallback.<PipelinesEventsInterpretedMessage, PipelinesEventsIndexedMessage>builder()
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
  public boolean isMessageCorrect(PipelinesEventsInterpretedMessage message) {
    // TODO:
    return true;
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index
   * pipeline
   */
  @Override
  public Runnable createRunnable(PipelinesEventsInterpretedMessage message) {
    return () -> {
      try {

        String indexName = computeIndexName(message);
        int numberOfShards = computeNumberOfShards();

        ProcessRunnerBuilder.ProcessRunnerBuilderBuilder builder =
            ProcessRunnerBuilder.builder()
                .config(config)
                .message(message)
                .esIndexName(indexName)
                .esAlias(config.indexConfig.occurrenceAlias)
                .esShardsNumber(numberOfShards);

        log.info("Start the process. Message - {}", message);
        runDistributed(message, builder);
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  @Override
  public PipelinesEventsIndexedMessage createOutgoingMessage(
      PipelinesEventsInterpretedMessage message) {
    return new PipelinesEventsIndexedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        message.getNumberOfRecords(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getEndpointType(),
        message.getValidationResult(),
        message.isValidator());
  }

  private void runDistributed(
      PipelinesEventsInterpretedMessage message,
      ProcessRunnerBuilder.ProcessRunnerBuilderBuilder builder)
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

  /** Computes the name for ES index. We always use an independent index for each dataset. */
  private String computeIndexName(PipelinesEventsInterpretedMessage message) throws IOException {
    // Independent index for datasets
    String datasetId = message.getDatasetUuid().toString();
    String idxName =
        datasetId + "_" + message.getAttempt() + "_" + config.indexConfig.occurrenceVersion;
    idxName = idxName + "_" + Instant.now().toEpochMilli();
    log.info("ES Index name - {}", idxName);
    return idxName;
  }

  private int computeNumberOfShards() {
    // TODO:
    return 10;
  }
}
