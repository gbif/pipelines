package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.Metrics.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.*;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.slf4j.MDC;

@Slf4j
public abstract class PipelinesQueueDrainerCallback<
        I extends PipelineBasedMessage, O extends PipelineBasedMessage>
    extends PipelinesCallback<I, O> implements MessageCallback<I> {

  protected LinkedHashMap<I, TrackingInfo> messagesBuffer = new LinkedHashMap<>();
  protected Long recordsBuffered = 0l;

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private static final long TIMEOUT_MS = 2 * 60 * 1000; // 2 minutes

  private long lastBufferDrainedTime = -1l;

  public PipelinesQueueDrainerCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String sparkMaster) {
    super(pipelinesConfig, publisher, sparkMaster);
    startTimeoutChecker();
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
      log.info("Processing attempt {}, queue size {}", message.getAttempt(), messagesBuffer.size());

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
      messagesBuffer.put(message, trackingInfo);
      recordsBuffered += ((PipelinesInterpretedMessage) message).getNumberOfRecords();

      if (messagesBuffer.size() >= 1000 || recordsBuffered >= 10_000) {

        log.info("Processing buffered messages, buffer size {}", messagesBuffer.size());
        handleBulkMessages(messagesBuffer.keySet().stream().toList());
        log.info("Finished processing buffered messages, buffer size {}", messagesBuffer.size());

        log.info(
            "Sending next messages to balancer for buffered messages, buffer size {}",
            messagesBuffer.size());

        bulkTrackingUpdate();

        messagesBuffer.clear();
        lastBufferDrainedTime = System.currentTimeMillis();
      }
    } catch (Exception e) {
      log.error("Error processing message for dataset batch. Will mark entire batch as failed");
      // FIXME send pipeline failed message for each message in buffer

    }
  }

  private void bulkTrackingUpdate() throws IOException {
    for (I message : messagesBuffer.keySet()) {
      TrackingInfo info = messagesBuffer.get(message);
      O outgoingMessage = createOutgoingMessage(message);

      String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
      String messagePayload = outgoingMessage.toString();

      publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));
      updateQueuedStatus(info, message);

      log.debug("Finished processing datasetKey: {}", message.getDatasetUuid());
    }
    log.info(
        "Finished sending next messages to balancer for buffered messages, buffer size {}",
        messagesBuffer.size());
  }

  public void startTimeoutChecker() {
    log.info("Starting timeout checker for buffered messages with timeout of {} ms", TIMEOUT_MS);
    scheduler.scheduleAtFixedRate(
        () -> {
          long now = System.currentTimeMillis();
          if (now - lastBufferDrainedTime > TIMEOUT_MS) {
            try {
              log.info(
                  "Buffer timeout reached, processing buffered messages, buffer size {}",
                  messagesBuffer.size());
              if (messagesBuffer.isEmpty()) {
                log.info("Buffer is empty, skipping processing");
              } else {
                handleBulkMessages(messagesBuffer.keySet().stream().toList());
                bulkTrackingUpdate();
                messagesBuffer.clear();
              }
            } catch (Exception e) {
              log.error("Error processing buffered messages, marking entire batch as failed", e);
              messagesBuffer.clear();
              // FIXME send pipeline failed message for each message in buffer
            }

            lastBufferDrainedTime = now;
          }
        },
        60,
        60,
        TimeUnit.SECONDS); // check every 60 seconds
  }
}
