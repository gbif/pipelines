package org.gbif.validator.orchestrator.step;

import java.util.Comparator;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.gbif.validator.orchestrator.service.OrchestrationService;
import org.gbif.validator.orchestrator.workflow.ValidationWorkflowMapper;
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

    // The coordinator owns persistence and workflow transitions. Job hooks and
    // message adaptation are executed only after the result has been persisted.
    WorkflowEvent event =
        result.successful() ? WorkflowEvent.completed(stepType) : WorkflowEvent.failed(stepType);
    coordinator.handle(
        result.validationKey(), event, registration.proposeChanges(validation, payload, reporter));

    JobContext context =
        new JobContext(
            result.validationKey(),
            result.attempt(),
            validation.getMetrics().getStepTypes().stream()
                .sorted(Comparator.comparingInt(ValidationStep::getExecutionOrder))
                .map(step -> StepType.valueOf(step.getStepType()))
                .toList());

    registration.runHooks(validation, payload, reporter);

    // Only successful jobs hand off to external processing infrastructure.
    if (result.successful() && coordinator.hasOutgoingDestination(stepType)) {
      Object outgoing = registration.adaptOutgoing(payload, context);
      coordinator.publishOutgoing(stepType, outgoing);
    }

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
