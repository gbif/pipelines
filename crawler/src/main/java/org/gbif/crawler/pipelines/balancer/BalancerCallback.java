package org.gbif.crawler.pipelines.balancer;

import java.io.IOException;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesHdfsViewBuiltMessage;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.crawler.pipelines.balancer.handler.InterpretedMessageHandler;
import org.gbif.crawler.pipelines.balancer.handler.PipelinesHdfsViewBuiltMessageHandler;
import org.gbif.crawler.pipelines.balancer.handler.PipelinesIndexedMessageHandler;
import org.gbif.crawler.pipelines.balancer.handler.VerbatimMessageHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesBalancerMessage} is received.
 * <p>
 * The general point is to populate a message with necessary fields and resend the message to the right service.
 * <p>
 * The main method is {@link BalancerCallback#handleMessage}
 */
public class BalancerCallback extends AbstractMessageCallback<PipelinesBalancerMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerCallback.class);

  private final BalancerConfiguration config;
  private final MessagePublisher publisher;

  BalancerCallback(BalancerConfiguration config, MessagePublisher publisher) {
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /** Handles a MQ {@link PipelinesBalancerMessage} message */
  @Override
  public void handleMessage(PipelinesBalancerMessage message) {
    LOG.info("Message handler began - {}", message);

    String className = message.getMessageClass();

    // Select handler by message class name
    try {
      if (PipelinesVerbatimMessage.class.getSimpleName().equals(className)) {
        VerbatimMessageHandler.handle(config, publisher, message);
      } else if (PipelinesInterpretedMessage.class.getSimpleName().equals(className)) {
        InterpretedMessageHandler.handle(config, publisher, message);
      } else if (PipelinesIndexedMessage.class.getSimpleName().equals(className)) {
        PipelinesIndexedMessageHandler.handle(config, publisher, message);
      } else if (PipelinesHdfsViewBuiltMessage.class.getSimpleName().equals(className)){
        PipelinesHdfsViewBuiltMessageHandler.handle(config, publisher, message);
      } else {
        LOG.error("Handler for {} wasn't found!", className);
      }
    } catch (IOException ex) {
      LOG.error("Exception during balancing the message", ex);
    }

    LOG.info("Message handler ended - {}", message);
  }

}
