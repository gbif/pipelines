package org.gbif.validator.orchestrator.service;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.orchestrator.workflow.WorkflowEvent;

/** Converts the analyser's existing JSON result to a workflow event. */
public final class DwcdpCompletionInterpreter {
  public record Completion(UUID validationKey, WorkflowEvent event) {}

  public Completion interpret(JsonNode message) {
    if (message == null || !message.isObject()) {
      throw new IllegalArgumentException("Expected a completion object");
    }

    JsonNode uuid = message.get("datasetUuid");
    JsonNode attempt = message.get("attempt");
    JsonNode valid = message.get("valid");
    JsonNode report = message.get("validationReport");
    if (uuid == null
        || !uuid.isTextual()
        || attempt == null
        || !attempt.isIntegralNumber()
        || attempt.intValue() != 1
        || valid == null
        || !valid.isBoolean()
        || report == null
        || report.isNull()) {
      throw new IllegalArgumentException("Incomplete or unsupported DWc-DP completion payload");
    }

    UUID key = UUID.fromString(uuid.textValue());

    // Policy for first integration: invalid datapackage terminates the workflow.
    WorkflowEvent event =
        valid.booleanValue()
            ? WorkflowEvent.completed(StepType.VALIDATOR_VALIDATE_DWCDP)
            : WorkflowEvent.failed(StepType.VALIDATOR_VALIDATE_DWCDP);
    return new Completion(key, event);
  }
}
