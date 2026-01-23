package org.gbif.pipelines.tasks.balancer.handler;

import static org.gbif.pipelines.tasks.balancer.handler.VerbatimMessageHandler.computeRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.pipelines.tasks.balancer.BalancerConfiguration;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EventsMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void handle(
      BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesEventsMessage - {}", message);

    // Populate message fields
    PipelinesEventsMessage outputMessage =
        MAPPER.readValue(message.getPayload(), PipelinesEventsMessage.class);

    StepRunner runner = computeRunner(config, outputMessage);
    outputMessage.setRunner(runner.name());

    publisher.send(outputMessage);
    log.info("The message has been sent - {}", outputMessage);
  }
}
