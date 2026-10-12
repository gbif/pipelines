package org.gbif.validator.orchestrator.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;
import org.junit.jupiter.api.Test;

class ValidationWorkerStepDefinitionTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void createsRequestForConfiguredThinWorkers() {
    UUID key = UUID.randomUUID();
    Validation validation = Validation.builder().key(key).build();

    for (StepType stepType : new StepType[] {
        StepType.VALIDATOR_DWCDP_STAGE,
        StepType.VALIDATOR_DWCDP_TO_VERBATIM,
        StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER}) {
      ValidationWorkerStepDefinition definition = new ValidationWorkerStepDefinition(stepType);

      assertEquals(Map.of("datasetUuid", key.toString(), "attempt", 1), definition.createRequest(validation));
      assertEquals(JsonNode.class, definition.completionType());
    }
  }

  @Test
  void normalizesSuccessfulAndFailedCompletions() {
    UUID key = UUID.randomUUID();
    ValidationWorkerStepDefinition definition =
        new ValidationWorkerStepDefinition(StepType.VALIDATOR_DWCDP_STAGE);

    CompletionResult completed = definition.interpret(message(key, true));
    CompletionResult failed = definition.interpret(message(key, false));

    assertEquals(key, completed.validationKey());
    assertEquals(1, completed.attempt());
    assertTrue(completed.successful());
    assertFalse(failed.successful());
  }

  @Test
  void rejectsIncompleteOrInvalidResults() {
    ValidationWorkerStepDefinition definition =
        new ValidationWorkerStepDefinition(StepType.VALIDATOR_DWCDP_TO_VERBATIM);

    assertThrows(IllegalArgumentException.class,
        () -> definition.interpret(mapper.createObjectNode().put("successful", true)));
    assertThrows(IllegalArgumentException.class,
        () -> definition.interpret(message(UUID.randomUUID(), true).deepCopy()
            .put("attempt", 0)));
    assertThrows(IllegalArgumentException.class,
        () -> definition.interpret(mapper.createObjectNode()
            .put("datasetUuid", UUID.randomUUID().toString())
            .put("attempt", 1)
            .put("successful", "true")));
  }

  @Test
  void normalizesIdentifierCompletion() {
    UUID key = UUID.randomUUID();
    ValidationWorkerStepDefinition definition =
        new ValidationWorkerStepDefinition(StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER);

    CompletionResult completed = definition.interpret(message(key, true));
    CompletionResult failed = definition.interpret(message(key, false));

    assertEquals(key, completed.validationKey());
    assertEquals(1, completed.attempt());
    assertTrue(completed.successful());
    assertFalse(failed.successful());
  }

  @Test
  void rejectsUnsupportedStepType() {
    assertThrows(IllegalArgumentException.class,
        () -> new ValidationWorkerStepDefinition(StepType.VALIDATOR_COLLECT_METRICS));
  }

  private ObjectNode message(UUID key, boolean successful) {
    return mapper.createObjectNode()
        .put("datasetUuid", key.toString())
        .put("attempt", 1)
        .put("successful", successful);
  }
}
