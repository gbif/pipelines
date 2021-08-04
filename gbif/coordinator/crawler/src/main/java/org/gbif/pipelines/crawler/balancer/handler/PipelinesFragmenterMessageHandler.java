package org.gbif.pipelines.crawler.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesFragmenterMessage;

/**
 * Populates and sends the {@link PipelinesFragmenterMessage} message, the main method is {@link
 * PipelinesFragmenterMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesFragmenterMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Main handler, basically computes the runner type and sends to the same consumer */
  public static void handle(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesIndexedMessage - {}", message);

    PipelinesFragmenterMessage m =
        MAPPER.readValue(message.getPayload(), PipelinesFragmenterMessage.class);

    PipelinesFragmenterMessage outputMessage =
        new PipelinesFragmenterMessage(
            m.getDatasetUuid(), m.getAttempt(), m.getPipelineSteps(), m.getExecutionId());

    publisher.send(outputMessage);

    log.info("The message has been sent - {}", outputMessage);
  }
}
