package org.gbif.crawler.pipelines.balancer.handler;

import java.io.IOException;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesHdfsViewBuiltMessage;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.crawler.pipelines.balancer.BalancerConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Populates and sends the {@link PipelinesIndexedMessage} message, the main method
 * is {@link PipelinesHdfsViewBuiltMessageHandler#handle}
 */
public class PipelinesHdfsViewBuiltMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PipelinesHdfsViewBuiltMessageHandler.class);

  private PipelinesHdfsViewBuiltMessageHandler() {
    // NOP
  }

  /**
   * Main handler, basically computes the runner type and sends to the same consumer
   */
  public static void handle(BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    LOG.info("Process PipelinesIndexedMessage - {}", message);

    ObjectMapper mapper = new ObjectMapper();
    PipelinesHdfsViewBuiltMessage m = mapper.readValue(message.getPayload(), PipelinesHdfsViewBuiltMessage.class);

    PipelinesHdfsViewBuiltMessage outputMessage =
        new PipelinesHdfsViewBuiltMessage(
            m.getDatasetUuid(),
            m.getAttempt(),
            m.getPipelineSteps(),
            m.getRunner(),
            m.getExecutionId());

    publisher.send(outputMessage);

    LOG.info("The message has been sent - {}", outputMessage);
  }
}
