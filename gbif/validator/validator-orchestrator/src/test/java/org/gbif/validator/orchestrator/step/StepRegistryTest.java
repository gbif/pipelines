package org.gbif.validator.orchestrator.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.junit.jupiter.api.Test;

class StepRegistryTest {

  private final DwcdpAnalysisStepDefinition definition = new DwcdpAnalysisStepDefinition();

  @Test
  void rejectsDuplicateStepRegistrations() {
    StepRegistration<JsonNode> registration = new StepRegistration<>(definition, List.of());

    assertThrows(IllegalArgumentException.class,
        () -> new StepRegistry(List.of(registration, registration)));
  }

  @Test
  void routesToExpectedDefinition() {
    StepRegistration<JsonNode> registration = new StepRegistration<>(definition, List.of());
    StepRegistry registry = new StepRegistry(List.of(registration));

    assertEquals(JsonNode.class,
        registry.require(StepType.VALIDATOR_VALIDATE_DWCDP).completionType());
  }

  @Test
  void optionalHookFailureDoesNotBlockOtherHooks() throws Exception {
    List<String> called = new ArrayList<>();
    StepRegistration<JsonNode> registration = new StepRegistration<>(definition, List.of(
        HookRegistration.optional((validation, completion) -> {
          throw new IllegalStateException("Optional failure");
        }),
        HookRegistration.required((validation, completion) -> {
          called.add("required");
        })));

    List<Exception> errors = new ArrayList<>();
    registration.runHooks(null, sampleMessage(), (step, exception) -> errors.add(exception));

    assertEquals(List.of("required"), called);
    assertEquals(1, errors.size());
  }

  @Test
  void requiredHookFailureStopsProcessing() {
    List<String> called = new ArrayList<>();
    StepRegistration<JsonNode> registration = new StepRegistration<>(definition, List.of(
        HookRegistration.required((validation, completion) -> {
          throw new IllegalStateException("Required failure");
        }),
        HookRegistration.optional((validation, completion) -> {
          called.add("not reached");
        })));

    assertThrows(IllegalStateException.class,
        () -> registration.runHooks(null, sampleMessage(), (step, exception) -> {}));
    assertTrue(called.isEmpty());
  }

  @Test
  void extractsCorrelationAndDomainOutcome() {
    CompletionResult result = definition.interpret(sampleMessage());

    assertEquals(1, result.attempt());
    assertTrue(result.successful());
  }

  private JsonNode sampleMessage() {
    return new ObjectMapper().createObjectNode()
        .put("datasetUuid", UUID.randomUUID().toString())
        .put("attempt", 1)
        .put("valid", true)
        .set("validationReport", new ObjectMapper().createObjectNode());
  }
}
