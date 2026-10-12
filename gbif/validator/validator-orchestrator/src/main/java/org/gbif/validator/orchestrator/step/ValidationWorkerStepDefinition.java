package org.gbif.validator.orchestrator.step;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;

/**
 * Contract for thin validation worker wrappers (stage, conversion and interpretation).
 *
 * <p>Request: datasetUuid and attempt. Completion: datasetUuid, attempt and successful.
 * These JSON fields are an explicit contract for the new validation wrappers; they are
 * not assumed to be the existing ingestion worker message format.
 */
public final class ValidationWorkerStepDefinition implements StepDefinition<JsonNode> {

  private final StepType stepType;

  public ValidationWorkerStepDefinition(StepType stepType) {
    Objects.requireNonNull(stepType, "stepType must not be null");

    if (stepType != StepType.VALIDATOR_DWCDP_STAGE
        && stepType != StepType.VALIDATOR_DWCDP_TO_VERBATIM
        && stepType != StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER) {
      throw new IllegalArgumentException("Unsupported validation wrapper step: " + stepType);
    }

    this.stepType = stepType;
  }

  @Override
  public StepType stepType() {
    return stepType;
  }

  @Override
  public Class<JsonNode> completionType() {
    return JsonNode.class;
  }

  @Override
  public Object createRequest(Validation validation) {
    Objects.requireNonNull(validation, "validation must not be null");

    return Map.of(
        "datasetUuid", Objects.requireNonNull(validation.getKey(), "validation key").toString(),
        "attempt", 1);
  }

  @Override
  public CompletionResult interpret(JsonNode completion) {
    if (completion == null || !completion.isObject()) {
      throw new IllegalArgumentException("Expected " + stepType + " completion object");
    }

    JsonNode datasetUuid = completion.path("datasetUuid");
    JsonNode attempt = completion.path("attempt");
    JsonNode successful = completion.path("successful");

    if (!datasetUuid.isTextual()
        || !attempt.isIntegralNumber()
        || !attempt.canConvertToInt()
        || attempt.intValue() < 1
        || !successful.isBoolean()) {
      throw new IllegalArgumentException("Invalid " + stepType + " completion payload");
    }

    UUID key = UUID.fromString(datasetUuid.textValue());
    boolean success = successful.booleanValue();

    return new CompletionResult(
        key,
        attempt.intValue(),
        success,
        success ? null : "Worker reported failure for " + stepType);
  }
}
