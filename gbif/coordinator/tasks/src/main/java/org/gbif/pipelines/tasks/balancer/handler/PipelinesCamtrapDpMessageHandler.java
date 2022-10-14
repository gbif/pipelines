package org.gbif.pipelines.tasks.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesCamtrapDpMessage;

/**
 * Populates and sends the {@link PipelinesCamtrapDpMessage} message, the main method is {@link
 * PipelinesCamtrapDpMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesCamtrapDpMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Main handler, basically computes the runner type and sends to the same consumer */
  public static void handle(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesCamtrapDpMessage - {}", message);

    PipelinesCamtrapDpMessage m =
        MAPPER.readValue(message.getPayload(), PipelinesCamtrapDpMessage.class);

    PipelinesCamtrapDpMessage outputMessage =
        new PipelinesCamtrapDpMessage(
            m.getDatasetUuid(),
            m.getDatasetType(),
            m.getSource(),
            m.getAttempt(),
            m.getValidationReport(),
            m.getPipelineSteps(),
            m.getEndpointType(),
            m.getPlatform(),
            m.getExecutionId());

    publisher.send(outputMessage);

    log.info("The message has been sent - {}", outputMessage);
  }
}
