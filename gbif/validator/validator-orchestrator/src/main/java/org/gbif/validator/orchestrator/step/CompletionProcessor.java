package org.gbif.validator.orchestrator.step;

import java.util.List;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.orchestrator.workflow.ValidationWorkflowMapper;
import org.gbif.validator.orchestrator.workflow.WorkflowState;
import org.gbif.validator.orchestrator.workflow.WorkflowChange;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;
import org.gbif.validator.orchestrator.service.OrchestrationService;
import org.gbif.validator.orchestrator.workflow.WorkflowEvent;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Runs step-specific processing, then delegates all state transitions to the coordinator. */
public final class CompletionProcessor {

  private final StepRegistry registry;
  private final ValidationWsClient client;
  private final OrchestrationService coordinator;
  private final HookFailureReporter reporter;

  public CompletionProcessor(StepRegistry registry, ValidationWsClient client,
      OrchestrationService coordinator, HookFailureReporter reporter) {
    this.registry = Objects.requireNonNull(registry);
    this.client = Objects.requireNonNull(client);
    this.coordinator = Objects.requireNonNull(coordinator);
    this.reporter = Objects.requireNonNull(reporter);
  }

  public void complete(StepType stepType, Object payload) throws Exception {
    StepRegistration<?> registration = registry.require(stepType);
    CompletionResult result = registration.interpret(payload);
    Validation validation = client.get(result.validationKey());

    if (validation == null) {
      throw new IllegalStateException("Unknown validation: " + result.validationKey());
    }

    if (result.attempt() != 1) {
      throw new IllegalArgumentException("Only attempt 1 is supported by validator-ws");
    }

    // Duplicate and late completions must not repeat external side effects.
    if (!isActive(validation, stepType)) {
      return;
    }

    // External completion artifacts must be persisted before advancing the workflow.
    registration.runHooks(validation, payload, reporter);
    List<WorkflowChange> changes = registration.proposeChanges(validation, payload, reporter);

    WorkflowEvent event = result.successful()
        ? WorkflowEvent.completed(stepType)
        : WorkflowEvent.failed(stepType);

    coordinator.handle(result.validationKey(), event, changes);
  }

  private boolean isActive(Validation validation, StepType stepType) {
    WorkflowState.Status workflowStatus = new ValidationWorkflowMapper().from(validation).status();

    if (workflowStatus == WorkflowState.Status.FINISHED
        || workflowStatus == WorkflowState.Status.FAILED
        || workflowStatus == WorkflowState.Status.ABORTED) {
      return false;
    }

    ValidationStep step = validation.getMetrics().getStepTypes().stream()
        .filter(candidate -> candidate.getStepType().equals(stepType.name()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Unknown validation step: " + stepType));

    return step.getStatus() == Validation.Status.QUEUED
        || step.getStatus() == Validation.Status.RUNNING;
  }
}
