package org.gbif.pipelines.tasks.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;

/**
 * Populates and sends the {@link
 * org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage} message, the main
 * method is {@link EventsInterpretedMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EventsInterpretedMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void handle(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesEventsInterpretedMessage - {}", message);

    // Populate message fields
    PipelinesEventsInterpretedMessage outputMessage =
        MAPPER.readValue(message.getPayload(), PipelinesEventsInterpretedMessage.class);

    publisher.send(outputMessage);
    log.info("The message has been sent - {}", outputMessage);
  }
}
