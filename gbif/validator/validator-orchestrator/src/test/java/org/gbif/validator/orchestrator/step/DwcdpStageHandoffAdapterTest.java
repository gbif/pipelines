package org.gbif.validator.orchestrator.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.messages.DwcDpStageMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.junit.jupiter.api.Test;

class DwcdpStageHandoffAdapterTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private final DwcdpStageHandoffAdapter adapter = new DwcdpStageHandoffAdapter(mapper);
  private final UUID validationKey = UUID.randomUUID();
  private final JobContext context =
      new JobContext(
          validationKey,
          1,
          List.of(
              StepType.VALIDATOR_VALIDATE_DWCDP,
              StepType.VALIDATOR_DWCDP_STAGE,
              StepType.VALIDATOR_DWCDP_TO_VERBATIM,
              StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
              StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
              StepType.VALIDATOR_COLLECT_METRICS));

  @Test
  void occurrenceOnly() throws Exception {
    DwcDpStageMessage stage = stage("occurrence");
    assertTrue(stage.isContainsOccurrences());
    assertFalse(stage.isContainsEvents());
    assertEquals(validationKey, stage.getDatasetUuid());
    assertEquals(1, stage.getAttempt());
    assertEquals(context.pipelineSteps().size(), stage.getPipelineSteps().size());
    assertTrue(stage.getPipelineSteps().contains(StepType.VALIDATOR_DWCDP_STAGE.name()));
    assertEquals(null, stage.getExecutionId());
  }

  @Test
  void eventOnly() throws Exception {
    DwcDpStageMessage stage = stage("event");
    assertFalse(stage.isContainsOccurrences());
    assertTrue(stage.isContainsEvents());
  }

  @Test
  void combinedResources() throws Exception {
    DwcDpStageMessage stage = stage("event", "occurrence");
    assertTrue(stage.isContainsOccurrences());
    assertTrue(stage.isContainsEvents());
  }

  @Test
  void rejectsMissingAndUnrecognizedResources() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> stage());
    assertThrows(IllegalArgumentException.class, () -> stage("measurement"));
    assertThrows(IllegalArgumentException.class, () -> stage(""));
  }

  @Test
  void rejectsMissingRequiredValidatorSteps() throws Exception {
    JobContext incomplete =
        new JobContext(validationKey, 1, List.of(StepType.VALIDATOR_VALIDATE_DWCDP));
    assertThrows(
        IllegalArgumentException.class, () -> adapter.adapt(completion("occurrence"), incomplete));
  }

  @Test
  void rejectsFailedCompletionAndMismatchedIdentity() throws Exception {
    JsonNode failed =
        mapper.readTree(
            completion("occurrence").toString().replace("\"valid\":true", "\"valid\":false"));
    assertThrows(IllegalArgumentException.class, () -> adapter.adapt(failed, context));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            adapter.adapt(
                completion("occurrence"),
                new JobContext(UUID.randomUUID(), 1, context.pipelineSteps())));
  }

  private DwcDpStageMessage stage(String... names) throws Exception {
    PipelinesBalancerMessage wrapper = adapter.adapt(completion(names), context);
    assertEquals(DwcDpStageMessage.class.getSimpleName(), wrapper.getMessageClass());
    return mapper.readValue(wrapper.getPayload(), DwcDpStageMessage.class);
  }

  private JsonNode completion(String... names) {
    com.fasterxml.jackson.databind.node.ObjectNode completion = mapper.createObjectNode();
    completion.put("datasetUuid", validationKey.toString());
    completion.put("attempt", 1);
    completion.put("valid", true);
    com.fasterxml.jackson.databind.node.ArrayNode resources =
        completion.putObject("validationReport").putArray("resourceAnalysisResults");
    for (String name : names) {
      resources.addObject().put("name", name);
    }
    return completion;
  }
}
