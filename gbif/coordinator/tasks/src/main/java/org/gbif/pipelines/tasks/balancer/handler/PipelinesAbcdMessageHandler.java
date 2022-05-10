package org.gbif.pipelines.tasks.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;

/**
 * Populates and sends the {@link PipelinesAbcdMessage} message, the main method is {@link
 * PipelinesAbcdMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesAbcdMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Main handler, basically computes the runner type and sends to the same consumer */
  public static void handle(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesAbcdMessage - {}", message);

    PipelinesAbcdMessage m = MAPPER.readValue(message.getPayload(), PipelinesAbcdMessage.class);

    PipelinesAbcdMessage outputMessage =
        new PipelinesAbcdMessage(
            m.getDatasetUuid(),
            m.getSource(),
            m.getAttempt(),
            m.isModified(),
            m.getPipelineSteps(),
            m.getEndpointType(),
            m.getExecutionId());

    publisher.send(outputMessage);

    log.info("The message has been sent - {}", outputMessage);
  }
}
