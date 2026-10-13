package org.gbif.validator.orchestrator.service;

import java.io.IOException;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.validator.orchestrator.config.OrchestratorProperties;
import org.springframework.stereotype.Service;

/** Routes a step-specific payload using configured transport destinations. */
@Service
public class StepDispatcher {
  private final MessagePublisher publisher;
  private final OrchestratorProperties properties;

  public StepDispatcher(MessagePublisher publisher, OrchestratorProperties properties) {
    this.publisher = Objects.requireNonNull(publisher);
    this.properties = Objects.requireNonNull(properties);
  }

  public void dispatch(StepType stepType, Object payload) throws IOException {
    Objects.requireNonNull(stepType, "stepType must not be null");
    Objects.requireNonNull(payload, "payload must not be null");
    OrchestratorProperties.Destination destination = properties.requireStep(stepType).getRequest();
    if (destination == null
        || isBlank(destination.getExchange())
        || isBlank(destination.getRoutingKey())) {
      throw new IllegalStateException("Missing request destination for step " + stepType);
    }
    publisher.send(payload, destination.getExchange(), destination.getRoutingKey());
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
