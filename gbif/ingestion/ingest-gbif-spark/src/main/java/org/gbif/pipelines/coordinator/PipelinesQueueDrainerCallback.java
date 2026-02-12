package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.Metrics.*;

import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.*;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.slf4j.MDC;

@Slf4j
public abstract class PipelinesQueueDrainerCallback<
        I extends PipelineBasedMessage, O extends PipelineBasedMessage>
    extends PipelinesCallback<I, O> implements MessageCallback<I> {

  protected List<I> messagesBuffer = new ArrayList<>();

  public PipelinesQueueDrainerCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String sparkMaster) {
    super(pipelinesConfig, publisher, sparkMaster);
  }

  protected abstract void handleBulkMessages(List<I> messages) throws Exception;

  @Override
  public void handleMessage(I message) {

    checkIfPaused();

    LAST_CONSUMED_MESSAGE_FROM_QUEUE_MS.set(System.currentTimeMillis());
    MESSAGES_READ_FROM_QUEUE.inc();

    MDC.put(
        "datasetKey",
        message.getDatasetUuid() != null ? message.getDatasetUuid().toString() : "NO_DATASET");
    log.debug("Received message: {}", message);

    if (!isMessageCorrect(message) || isProcessingStopped(message)) {

      log.debug(
          "Returning message correct: {} isProcessingStopped: {}",
          isMessageCorrect(message),
          isProcessingStopped(message));
      return;
    }

    TrackingInfo trackingInfo = null;

    try (MDC.MDCCloseable mdc =
            MDC.putCloseable("datasetKey", message.getDatasetUuid().toString());
        MDC.MDCCloseable mdc1 = MDC.putCloseable("attempt", message.getAttempt().toString());
        MDC.MDCCloseable mdc2 = MDC.putCloseable("step", getStepType().name())) {
      log.info("Processing attempt {}", message.getAttempt());

      trackingInfo = trackPipelineStep(message);

    } catch (Exception e) {
      log.error("Error processing message for dataset {}", message.getDatasetUuid(), e);
      if (trackingInfo != null) {
        updateTrackingStatus(trackingInfo, message, PipelineStep.Status.FAILED);
      }
      return;
    }

    try {
      CONCURRENT_DATASETS.inc();
      messagesBuffer.add(message);

      if (messagesBuffer.size() < 100) {
        log.info("Processing buffered messages, buffer size {}", messagesBuffer.size());
        handleBulkMessages(messagesBuffer);
        messagesBuffer.clear();
      }
    } catch (Exception e) {
      log.error("Error processing message for dataset batch. Will mark entire batch as failed");

      // TOOD

    }
  }
}
