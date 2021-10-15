package org.gbif.pipelines.tasks.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesHdfsViewBuiltMessage;

/**
 * Populates and sends the {@link PipelinesHdfsViewBuiltMessage} message, the main method is {@link
 * PipelinesHdfsViewBuiltMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesHdfsViewBuiltMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Main handler, basically computes the runner type and sends to the same consumer */
  public static void handle(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesIndexedMessage - {}", message);

    PipelinesHdfsViewBuiltMessage m =
        MAPPER.readValue(message.getPayload(), PipelinesHdfsViewBuiltMessage.class);

    PipelinesHdfsViewBuiltMessage outputMessage =
        new PipelinesHdfsViewBuiltMessage(
            m.getDatasetUuid(),
            m.getAttempt(),
            m.getPipelineSteps(),
            m.getRunner(),
            m.getExecutionId());

    publisher.send(outputMessage);

    log.info("The message has been sent - {}", outputMessage);
  }
}
