package org.gbif.validator.orchestrator.step;

import java.util.List;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.gbif.validator.orchestrator.service.OrchestrationService;
import org.gbif.validator.orchestrator.workflow.ValidationWorkflowMapper;
import org.gbif.validator.orchestrator.workflow.WorkflowChange;
import org.gbif.validator.orchestrator.workflow.WorkflowEvent;
import org.gbif.validator.orchestrator.workflow.WorkflowState;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs step-specific processing, then delegates all state transitions to the coordinator. */
public final class CompletionProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(CompletionProcessor.class);

  private final StepRegistry registry;
  private final ValidationWsClient client;
  private final OrchestrationService coordinator;
  private final HookFailureReporter reporter;

  public CompletionProcessor(
      StepRegistry registry,
      ValidationWsClient client,
      OrchestrationService coordinator,
      HookFailureReporter reporter) {
    this.registry = Objects.requireNonNull(registry);
    this.client = Objects.requireNonNull(client);
    this.coordinator = Objects.requireNonNull(coordinator);
    this.reporter = Objects.requireNonNull(reporter);
  }

  public void complete(StepType stepType, Object payload) throws Exception {
    StepRegistration<?> registration = registry.require(stepType);
    CompletionResult result = registration.interpret(payload);
    LOG.info(
        "Received step completion validation={} attempt={} step={} successful={}",
        result.validationKey(),
        result.attempt(),
        stepType,
        result.successful());
    Validation validation = client.get(result.validationKey());

    if (validation == null) {
      throw new IllegalStateException("Unknown validation: " + result.validationKey());
    }

    if (result.attempt() != 1) {
      throw new IllegalArgumentException("Only attempt 1 is supported by validator-ws");
    }

    // Duplicate and late completions must not repeat external side effects.
    if (!isActive(validation, stepType)) {
      LOG.info(
          "Ignoring late or duplicate completion validation={} step={}",
          result.validationKey(),
          stepType);
      return;
    }

    // External completion artifacts must be persisted before advancing the workflow.
    List<WorkflowChange> changes;

    try {
      registration.runHooks(validation, payload, reporter);
      changes = registration.proposeChanges(validation, payload, reporter);
    } catch (Exception hookFailure) {
      LOG.error(
          "Required completion processing failed validation={} step={}",
          result.validationKey(),
          stepType,
          hookFailure);
      // Required hooks are part of the execution. With no automatic retries,
      // a failed required hook must not leave its step indefinitely queued.
      try {
        coordinator.handle(result.validationKey(), WorkflowEvent.failed(stepType));
      } catch (Exception transitionFailure) {
        hookFailure.addSuppressed(transitionFailure);
      }

      throw hookFailure;
    }

    WorkflowEvent event =
        result.successful() ? WorkflowEvent.completed(stepType) : WorkflowEvent.failed(stepType);

    coordinator.handle(result.validationKey(), event, changes);
    LOG.info(
        "Step completion processed validation={} step={} result={}",
        result.validationKey(),
        stepType,
        event.kind());
  }

  private boolean isActive(Validation validation, StepType stepType) {
    WorkflowState.Status workflowStatus = new ValidationWorkflowMapper().from(validation).status();

    if (workflowStatus == WorkflowState.Status.FINISHED
        || workflowStatus == WorkflowState.Status.FAILED
        || workflowStatus == WorkflowState.Status.ABORTED) {
      return false;
    }

    ValidationStep step =
        validation.getMetrics().getStepTypes().stream()
            .filter(candidate -> candidate.getStepType().equals(stepType.name()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Unknown validation step: " + stepType));

    return step.getStatus() == Validation.Status.QUEUED
        || step.getStatus() == Validation.Status.RUNNING;
  }
}
