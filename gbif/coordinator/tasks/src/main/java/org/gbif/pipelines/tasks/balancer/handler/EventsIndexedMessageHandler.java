package org.gbif.pipelines.tasks.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsIndexedMessage;

/**
 * Populates and sends the {@link
 * org.gbif.common.messaging.api.messages.PipelinesEventsIndexedMessage} message, the main method is
 * {@link EventsIndexedMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EventsIndexedMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void handle(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesEventsIndexedMessage - {}", message);

    // Populate message fields
    PipelinesEventsIndexedMessage outputMessage =
        MAPPER.readValue(message.getPayload(), PipelinesEventsIndexedMessage.class);

    publisher.send(outputMessage);
    log.info("The message has been sent - {}", outputMessage);
  }
}
