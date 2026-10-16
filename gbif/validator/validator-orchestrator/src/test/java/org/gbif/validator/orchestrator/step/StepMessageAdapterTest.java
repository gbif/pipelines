package org.gbif.validator.orchestrator.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.junit.jupiter.api.Test;

class StepMessageAdapterTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private final JobContext context =
      new JobContext(UUID.randomUUID(), 1, List.of(StepType.VALIDATOR_VALIDATE_DWCDP));

  @Test
  void defaultOutgoingAdapterForwardsUnchanged() throws Exception {
    StepRegistration<JsonNode> registration =
        StepRegistration.builder(new DwcdpAnalysisStepDefinition()).build();
    JsonNode completion = mapper.createObjectNode().put("valid", true);

    assertSame(completion, registration.adaptOutgoing(completion, context));
  }

  @Test
  void customOutgoingAdapterOverridesForwarding() throws Exception {
    StepRegistration<JsonNode> registration =
        StepRegistration.builder(new DwcdpAnalysisStepDefinition())
            .adaptOutgoing((completion, metadata) -> metadata.datasetUuid().toString())
            .build();

    assertEquals(context.datasetUuid().toString(),
        registration.adaptOutgoing(mapper.createObjectNode(), context));
  }

  @Test
  void requestAdapterOverridesUnchangedRequest() throws Exception {
    StepRegistration<JsonNode> registration =
        StepRegistration.builder(new DwcdpAnalysisStepDefinition())
            .before((request, metadata) -> "adapted:" + metadata.attempt())
            .build();

    assertEquals("adapted:1", registration.adaptRequest("original", context));
  }

  @Test
  void hooksAreOptInAndOrdered() throws Exception {
    List<String> executions = new java.util.ArrayList<>();
    StepRegistration<JsonNode> registration =
        StepRegistration.builder(new DwcdpAnalysisStepDefinition())
            .onJobProcessed(HookRegistration.required((validation, result) -> executions.add("first")))
            .onJobProcessed(HookRegistration.required((validation, result) -> executions.add("second")))
            .build();

    registration.runHooks(null, mapper.createObjectNode(), (step, error) -> {});

    assertEquals(List.of("first", "second"), executions);
  }
}
