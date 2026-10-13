package org.gbif.validator.orchestrator.step;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;

/** Keeps the existing analyser JSON request/completion wire contracts unchanged. */
public final class DwcdpAnalysisStepDefinition implements StepDefinition<JsonNode> {

  @Override
  public StepType stepType() {
    return StepType.VALIDATOR_VALIDATE_DWCDP;
  }

  @Override
  public Class<JsonNode> completionType() {
    return JsonNode.class;
  }

  @Override
  public Object createRequest(Validation validation) {
    return Map.of("datasetUuid", validation.getKey().toString(), "attempt", 1);
  }

  @Override
  public CompletionResult interpret(JsonNode message) {
    if (message == null || !message.isObject()) {
      throw new IllegalArgumentException("Expected a DWc-DP completion object");
    }

    JsonNode uuid = message.path("datasetUuid");
    JsonNode attempt = message.path("attempt");
    JsonNode valid = message.path("valid");
    JsonNode report = message.path("validationReport");

    if (!uuid.isTextual()
        || !attempt.isIntegralNumber()
        || !valid.isBoolean()
        || report.isMissingNode()
        || report.isNull()) {
      throw new IllegalArgumentException("Incomplete DWc-DP completion payload");
    }

    return new CompletionResult(
        UUID.fromString(uuid.textValue()),
        attempt.intValue(),
        valid.booleanValue(),
        valid.booleanValue() ? null : "Invalid DWc-DP");
  }
}
