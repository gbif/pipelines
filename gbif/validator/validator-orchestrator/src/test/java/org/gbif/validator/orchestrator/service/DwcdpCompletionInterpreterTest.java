package org.gbif.validator.orchestrator.service;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.orchestrator.workflow.WorkflowEvent;
import org.junit.jupiter.api.Test;

class DwcdpCompletionInterpreterTest {
  private final ObjectMapper mapper = new ObjectMapper();
  private final DwcdpCompletionInterpreter interpreter = new DwcdpCompletionInterpreter();
  private final UUID key = UUID.randomUUID();

  private com.fasterxml.jackson.databind.JsonNode message(boolean valid) {
    return mapper.createObjectNode().put("datasetUuid", key.toString())
        .put("attempt", 1).put("valid", valid)
        .set("validationReport", mapper.createObjectNode().put("count", 2));
  }

  @Test void validReportCompletesAnalyserStep() {
    var completion = interpreter.interpret(message(true));
    assertEquals(key, completion.validationKey());
    assertEquals(WorkflowEvent.Kind.COMPLETED, completion.event().kind());
    assertEquals(StepType.VALIDATOR_VALIDATE_DWCDP, completion.event().step());
  }

  @Test void invalidReportStopsWorkflowUnderInitialPolicy() {
    assertEquals(WorkflowEvent.Kind.FAILED, interpreter.interpret(message(false)).event().kind());
  }

  @Test void rejectsMissingReport() {
    var input = ((com.fasterxml.jackson.databind.node.ObjectNode) message(true));
    input.remove("validationReport");
    assertThrows(IllegalArgumentException.class, () -> interpreter.interpret(input));
  }

  @Test void rejectsOtherAttempts() {
    var input = ((com.fasterxml.jackson.databind.node.ObjectNode) message(true));
    input.put("attempt", 2);
    assertThrows(IllegalArgumentException.class, () -> interpreter.interpret(input));
  }

  @Test void rejectsMalformedUuid() {
    var input = ((com.fasterxml.jackson.databind.node.ObjectNode) message(true));
    input.put("datasetUuid", "bad");
    assertThrows(IllegalArgumentException.class, () -> interpreter.interpret(input));
  }
}
