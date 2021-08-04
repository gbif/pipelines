package org.gbif.pipelines.crawler.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;

/**
 * Populates and sends the {@link PipelinesXmlMessage} message, the main method is {@link
 * PipelinesXmlMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesXmlMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Main handler, basically computes the runner type and sends to the same consumer */
  public static void handle(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesXmlMessage - {}", message);

    PipelinesXmlMessage m = MAPPER.readValue(message.getPayload(), PipelinesXmlMessage.class);

    PipelinesXmlMessage outputMessage =
        new PipelinesXmlMessage(
            m.getDatasetUuid(),
            m.getAttempt(),
            m.getTotalRecordCount(),
            m.getReason(),
            m.getPipelineSteps(),
            m.getEndpointType(),
            m.getPlatform(),
            m.getExecutionId(),
            m.isValidator());

    publisher.send(outputMessage);

    log.info("The message has been sent - {}", outputMessage);
  }
}
