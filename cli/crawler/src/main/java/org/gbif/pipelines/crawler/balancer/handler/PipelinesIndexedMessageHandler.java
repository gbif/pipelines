package org.gbif.pipelines.crawler.balancer.handler;

import java.io.IOException;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.pipelines.crawler.balancer.BalancerConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Populates and sends the {@link PipelinesIndexedMessage} message, the main method
 * is {@link PipelinesIndexedMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesIndexedMessageHandler {

  /**
   * Main handler, basically computes the runner type and sends to the same consumer
   */
  public static void handle(BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesIndexedMessage - {}", message);

    ObjectMapper mapper = new ObjectMapper();
    PipelinesIndexedMessage m = mapper.readValue(message.getPayload(), PipelinesIndexedMessage.class);

    PipelinesIndexedMessage outputMessage =
        new PipelinesIndexedMessage(
            m.getDatasetUuid(),
            m.getAttempt(),
            m.getPipelineSteps(),
            m.getRunner(),
            m.getExecutionId());

    publisher.send(outputMessage);

    log.info("The message has been sent - {}", outputMessage);
  }
}
