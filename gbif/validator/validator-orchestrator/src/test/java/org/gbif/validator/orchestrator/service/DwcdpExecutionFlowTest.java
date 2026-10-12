package org.gbif.validator.orchestrator.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.orchestrator.config.DagProperties;
import org.gbif.validator.orchestrator.config.OrchestratorProperties;
import org.gbif.validator.orchestrator.step.CompletionProcessor;
import org.gbif.validator.orchestrator.step.DwcdpAnalysisStepDefinition;
import org.gbif.validator.orchestrator.step.DwcdpValidationReportHook;
import org.gbif.validator.orchestrator.step.HookRegistration;
import org.gbif.validator.orchestrator.step.StepRegistration;
import org.gbif.validator.orchestrator.step.StepRegistry;
import org.gbif.validator.orchestrator.support.RecordingValidationClient;
import org.gbif.validator.orchestrator.workflow.WorkflowEvent;
import org.junit.jupiter.api.Test;

/** Exercises the real coordinator and completion processor with isolated transport fakes. */
class DwcdpExecutionFlowTest {

  @Test
  void startsUploadsReportAndFinishesSingleStep() throws Exception {
    Fixture fixture = new Fixture();

    fixture.coordinator.handle(fixture.key, WorkflowEvent.start());

    assertEquals(Validation.Status.QUEUED, fixture.step().getStatus());
    assertEquals(1, fixture.messages.size());
    assertEquals(1, fixture.client.getStatusWrites());

    fixture.processor.complete(StepType.VALIDATOR_VALIDATE_DWCDP, fixture.completion(true));

    assertEquals(Validation.Status.FINISHED, fixture.step().getStatus());
    assertEquals(Validation.Status.FINISHED, fixture.client.getStoredValidation().getStatus());
    assertEquals(1, fixture.client.getReportWrites());
    assertEquals(1, fixture.messages.size());

    fixture.processor.complete(StepType.VALIDATOR_VALIDATE_DWCDP, fixture.completion(true));

    assertEquals(1, fixture.client.getReportWrites());
    assertEquals(2, fixture.client.getStatusWrites());
  }

  @Test
  void failedReportUploadFailsExecutionWithoutDispatchingNextStep() throws Exception {
    Fixture fixture = new Fixture();
    fixture.coordinator.handle(fixture.key, WorkflowEvent.start());
    fixture.client.setRejectReport(true);

    assertThrows(IllegalStateException.class, () -> fixture.processor.complete(
        StepType.VALIDATOR_VALIDATE_DWCDP, fixture.completion(true)));

    assertEquals(Validation.Status.FAILED, fixture.step().getStatus());
    assertEquals(Validation.Status.FAILED, fixture.client.getStoredValidation().getStatus());
    assertEquals(0, fixture.client.getReportWrites());
    assertEquals(1, fixture.messages.size());
    assertEquals(2, fixture.client.getStatusWrites());
  }

  @Test
  void invalidDataUploadsReportAndMarksWorkflowFailed() throws Exception {
    Fixture fixture = new Fixture();
    fixture.coordinator.handle(fixture.key, WorkflowEvent.start());

    fixture.processor.complete(StepType.VALIDATOR_VALIDATE_DWCDP, fixture.completion(false));

    assertEquals(Validation.Status.FAILED, fixture.step().getStatus());
    assertEquals(Validation.Status.FAILED, fixture.client.getStoredValidation().getStatus());
    assertEquals(1, fixture.client.getReportWrites());
  }

  @Test
  void completesAnalysisWithoutQueuingUnmigratedSteps() throws Exception {
    Fixture fixture = new Fixture(true);

    fixture.coordinator.handle(fixture.key, WorkflowEvent.start());
    fixture.processor.complete(StepType.VALIDATOR_VALIDATE_DWCDP, fixture.completion(true));

    assertEquals(Validation.Status.FINISHED, fixture.step().getStatus());
    assertEquals(null, fixture.client.getStoredValidation().getMetrics().getStepTypes()
        .get(1).getStatus());
    assertEquals(Validation.Status.RUNNING, fixture.client.getStoredValidation().getStatus());
    assertEquals(1, fixture.client.getReportWrites());
    assertEquals(1, fixture.messages.size());
  }

  private static final class Fixture {

    private final UUID key = UUID.randomUUID();
    private final RecordingValidationClient client = new RecordingValidationClient();
    private final List<Object> messages = new ArrayList<>();
    private final OrchestrationService coordinator;
    private final CompletionProcessor processor;

    private Fixture() {
      this(false);
    }

    private Fixture(boolean includeLegacyStep) {
      Metrics.ValidationStep first = Metrics.ValidationStep.builder()
          .stepType(StepType.VALIDATOR_VALIDATE_DWCDP.name())
          .executionOrder(1)
          .build();
      List<Metrics.ValidationStep> steps = new ArrayList<>();
      steps.add(first);

      if (includeLegacyStep) {
        steps.add(Metrics.ValidationStep.builder()
            .stepType(StepType.VALIDATOR_DWCDP_STAGE.name())
            .executionOrder(2)
            .build());
      }

      client.setValidation(Validation.builder()
          .key(key)
          .status(Validation.Status.QUEUED)
          .metrics(Metrics.builder().stepTypes(steps).build())
          .build());

      OrchestratorProperties properties = new OrchestratorProperties();
      OrchestratorProperties.StepMessaging messaging = new OrchestratorProperties.StepMessaging();
      OrchestratorProperties.Destination destination = new OrchestratorProperties.Destination();
      destination.setExchange("validation.jobs");
      destination.setRoutingKey("dwcdp.request");
      messaging.setRequest(destination);
      properties.setSteps(Map.of(StepType.VALIDATOR_VALIDATE_DWCDP, messaging));

      MessagePublisher publisher = (MessagePublisher) Proxy.newProxyInstance(
          MessagePublisher.class.getClassLoader(),
          new Class<?>[] {MessagePublisher.class},
          (proxy, method, args) -> {
            if (method.getName().equals("send")) {
              messages.add(args[0]);
              return null;
            }

            throw new UnsupportedOperationException(method.getName());
          });

      StepRegistry registry = new StepRegistry(List.of(new StepRegistration<>(
          new DwcdpAnalysisStepDefinition(),
          List.of(HookRegistration.required(new DwcdpValidationReportHook(client))))));

      coordinator = new OrchestrationService(
          client, new StepDispatcher(publisher, properties), registry, new DagProperties());
      processor = new CompletionProcessor(registry, client, coordinator, (step, error) -> {
        throw new AssertionError("Unexpected optional hook failure", error);
      });
    }

    private Metrics.ValidationStep step() {
      return client.getStoredValidation().getMetrics().getStepTypes().get(0);
    }

    private JsonNode completion(boolean valid) {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode node = mapper.createObjectNode();
      node.put("datasetUuid", key.toString());
      node.put("attempt", 1);
      node.put("valid", valid);
      node.set("validationReport", mapper.createObjectNode().put("records", 3));
      return node;
    }
  }

}
