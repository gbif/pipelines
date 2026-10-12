package org.gbif.validator.orchestrator.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.gbif.api.model.pipelines.StepType;
import org.junit.jupiter.api.Test;

class StepCompletionCallbackTest {

  @Test
  void forwardsStepAndTypedMessage() {
    List<StepType> steps = new ArrayList<>();
    List<Object> messages = new ArrayList<>();
    JsonNode message = new ObjectMapper().createObjectNode();

    StepCompletionCallback<JsonNode> callback = new StepCompletionCallback<>(
        StepType.VALIDATOR_VALIDATE_DWCDP,
        JsonNode.class,
        (step, payload) -> {
          steps.add(step);
          messages.add(payload);
        });

    callback.handleMessage(message);

    assertEquals(JsonNode.class, callback.getMessageClass());
    assertEquals(List.of(StepType.VALIDATOR_VALIDATE_DWCDP), steps);
    assertSame(message, messages.get(0));
  }

  @Test
  void wrapsCheckedExceptionWithoutRetrying() {
    Exception failure = new Exception("Report upload failed");
    StepCompletionCallback<JsonNode> callback = new StepCompletionCallback<>(
        StepType.VALIDATOR_VALIDATE_DWCDP,
        JsonNode.class,
        (step, payload) -> {
          throw failure;
        });

    OrchestrationProcessingException exception = assertThrows(
        OrchestrationProcessingException.class,
        () -> callback.handleMessage(new ObjectMapper().createObjectNode()));

    assertSame(failure, exception.getCause());
  }
}
