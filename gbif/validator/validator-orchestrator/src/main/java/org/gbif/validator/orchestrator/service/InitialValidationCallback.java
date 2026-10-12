package org.gbif.validator.orchestrator.service;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;
import java.util.UUID;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.validator.orchestrator.workflow.WorkflowEvent;
import org.springframework.stereotype.Component;

/** Starts a validation using persisted workflow state, not steps embedded in the request. */
@Component
public class InitialValidationCallback implements MessageCallback<JsonNode> {

  private final OrchestrationService coordinator;

  public InitialValidationCallback(OrchestrationService coordinator) {
    this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
  }

  @Override
  public Class<JsonNode> getMessageClass() {
    return JsonNode.class;
  }

  @Override
  public void handleMessage(JsonNode message) {
    try {
      String key = message.path("datasetUuid").textValue();

      if (key == null) {
        throw new IllegalArgumentException("Missing datasetUuid");
      }

      JsonNode attemptNode = message.path("attempt");
      if (!attemptNode.isMissingNode() && !attemptNode.isIntegralNumber()) {
        throw new IllegalArgumentException("Invalid attempt");
      }

      int attempt = attemptNode.isMissingNode() ? 1 : attemptNode.intValue();

      if (attempt != 1) {
        throw new IllegalArgumentException("validator-ws only supports attempt 1");
      }

      coordinator.handle(UUID.fromString(key), WorkflowEvent.start());
    } catch (Exception exception) {
      throw new OrchestrationProcessingException("Failed to start validation", exception);
    }
  }
}
