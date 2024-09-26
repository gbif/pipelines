package org.gbif.pipelines.tasks.balancer;

import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.*;
import org.gbif.pipelines.tasks.balancer.handler.*;

/**
 * Callback which is called when the {@link PipelinesBalancerMessage} is received.
 *
 * <p>The general point is to populate a message with necessary fields and resend the message to the
 * right service.
 *
 * <p>The main method is {@link BalancerCallback#handleMessage}
 */
@Slf4j
@AllArgsConstructor
public class BalancerCallback extends AbstractMessageCallback<PipelinesBalancerMessage> {

  @NonNull private final BalancerConfiguration config;
  private final MessagePublisher publisher;

  /** Handles a MQ {@link PipelinesBalancerMessage} message */
  @Override
  public void handleMessage(PipelinesBalancerMessage message) {
    log.info("Message handler began - {}", message);

    String className = message.getMessageClass();

    // Select handler by message class name
    try {
      if (PipelinesVerbatimMessage.class.getSimpleName().equals(className)) {
        VerbatimMessageHandler.handle(config, publisher, message);
      } else if (PipelinesInterpretedMessage.class.getSimpleName().equals(className)) {
        InterpretedMessageHandler.handle(config, publisher, message);
      } else if (PipelinesIndexedMessage.class.getSimpleName().equals(className)) {
        PipelinesIndexedMessageHandler.handle(publisher, message);
      } else if (PipelinesHdfsViewMessage.class.getSimpleName().equals(className)) {
        PipelinesHdfsViewMessageHandler.handle(publisher, message);
      } else if (PipelinesDwcaMessage.class.getSimpleName().equals(className)) {
        PipelinesDwcaMessageHandler.handle(publisher, message);
      } else if (PipelinesXmlMessage.class.getSimpleName().equals(className)) {
        PipelinesXmlMessageHandler.handle(publisher, message);
      } else if (PipelinesAbcdMessage.class.getSimpleName().equals(className)) {
        PipelinesAbcdMessageHandler.handle(publisher, message);
      } else if (PipelinesFragmenterMessage.class.getSimpleName().equals(className)) {
        PipelinesFragmenterMessageHandler.handle(publisher, message);
      } else if (PipelinesEventsInterpretedMessage.class.getSimpleName().equals(className)) {
        EventsInterpretedMessageHandler.handle(publisher, message);
      } else if (PipelinesEventsIndexedMessage.class.getSimpleName().equals(className)) {
        EventsIndexedMessageHandler.handle(publisher, message);
      } else if (PipelinesEventsHdfsViewMessage.class.getSimpleName().equals(className)) {
        PipelinesEventsHdfsViewMessageHandler.handle(publisher, message);
      } else if (DataWarehouseMessage.class.getSimpleName().equals(className)) {
        PipelinesWarehouseMessageHandler.handle(publisher, message);
      } else {
        log.error("Handler for {} wasn't found!", className);
      }
    } catch (IOException ex) {
      log.error("Exception during balancing the message", ex);
    }

    log.info("Message handler ended - {}", message);
  }
}
