package org.gbif.validator.orchestrator.service;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.gbif.validator.orchestrator.config.DagProperties;
import org.gbif.validator.orchestrator.step.StepRegistry;
import org.gbif.validator.orchestrator.step.JobContext;
import org.gbif.validator.orchestrator.workflow.DagWorkflowEngine;
import org.gbif.validator.orchestrator.workflow.SequentialWorkflowEngine;
import org.gbif.validator.orchestrator.workflow.ValidationWorkflowMapper;
import org.gbif.validator.orchestrator.workflow.WorkflowChange;
import org.gbif.validator.orchestrator.workflow.WorkflowDecision;
import org.gbif.validator.orchestrator.workflow.WorkflowEvent;
import org.gbif.validator.orchestrator.workflow.WorkflowState;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Connects the pure engine to validator-ws and a message dispatcher. Not a safe concurrent/atomic
 * execution gate: validator-ws exposes full-object PUT, not conditional status transitions, and
 * RabbitMQ publishes are not in that transaction. Callers must serialize events per validation and
 * arrange recovery for publish failures.
 */
@Service
public class OrchestrationService {

  private static final Logger LOG = LoggerFactory.getLogger(OrchestrationService.class);
  private final ValidationWsClient client;
  private final StepDispatcher dispatcher;
  private final StepRegistry stepRegistry;
  private final ValidationWorkflowMapper mapper = new ValidationWorkflowMapper();
  private final SequentialWorkflowEngine sequentialEngine = new SequentialWorkflowEngine();
  private final DagProperties dagProperties;

  public OrchestrationService(
      ValidationWsClient client,
      StepDispatcher dispatcher,
      StepRegistry stepRegistry,
      DagProperties dagProperties) {
    this.client = Objects.requireNonNull(client);
    this.dispatcher = Objects.requireNonNull(dispatcher);
    this.stepRegistry = Objects.requireNonNull(stepRegistry);
    this.dagProperties = Objects.requireNonNull(dagProperties);
  }

  /** Side-effect-free plan based on the latest server snapshot. */
  public WorkflowDecision plan(UUID validationKey, WorkflowEvent event) {
    return evaluate(mapper.from(client.get(validationKey)), event, List.of());
  }

  /**
   * Applies a plan by saving status changes before dispatch. Not invoked by an MQ listener until
   * conditional transitions and a recovery strategy are added.
   */
  public void handle(UUID validationKey, WorkflowEvent event) throws IOException {
    handle(validationKey, event, List.of());
  }

  public void handle(UUID validationKey, WorkflowEvent event, List<WorkflowChange> changes)
      throws IOException {
    LOG.info(
        "Processing workflow event validation={} event={} step={}",
        validationKey,
        event.kind(),
        event.step());
    Validation validation = client.get(validationKey);
    WorkflowDecision decision = evaluate(mapper.from(validation), event, changes);

    // Phase 1 owns only registered workers. Existing validation pipeline steps retain
    // their persisted state until the legacy pipeline handoff is implemented.
    Set<StepType> unownedDispatches =
        decision.actions().stream()
            .filter(action -> action.kind() == WorkflowDecision.Kind.DISPATCH)
            .map(WorkflowDecision.Action::step)
            .filter(step -> !stepRegistry.supports(step))
            .collect(Collectors.toSet());

    if (!unownedDispatches.isEmpty()) {
      LOG.info(
          "Leaving unowned steps for legacy handoff validation={} steps={}",
          validationKey,
          unownedDispatches);
    }

    List<WorkflowDecision.Action> actions =
        decision.actions().stream()
            .filter(
                action ->
                    !(action.kind() == WorkflowDecision.Kind.DISPATCH
                        && unownedDispatches.contains(action.step())))
            .filter(
                action ->
                    !(action.kind() == WorkflowDecision.Kind.SET_STEP_STATUS
                        && unownedDispatches.contains(action.step())
                        && action.status() == WorkflowState.Status.QUEUED))
            .toList();

    // Resolve payloads up-front to avoid changing state for unsupported steps.
    Map<StepType, Object> requests = new EnumMap<>(StepType.class);
    for (WorkflowDecision.Action action : actions) {
      if (action.kind() == WorkflowDecision.Kind.DISPATCH) {
        Object request = stepRegistry.require(action.step()).createRequest(validation);
        try {
          request = stepRegistry.require(action.step()).adaptRequest(
              request, jobContext(validation, 1));
        } catch (Exception exception) {
          throw new IOException("Failed to adapt request for " + action.step(), exception);
        }
        requests.put(action.step(), request);
      }
    }

    boolean changed = false;
    for (WorkflowDecision.Action action : actions) {
      switch (action.kind()) {
        case SET_STEP_STATUS -> {
          ValidationStep step =
              validation.getMetrics().getStepTypes().stream()
                  .filter(s -> s.getStepType().equals(action.step().name()))
                  .findFirst()
                  .orElseThrow();
          Validation.Status previousStatus = step.getStatus();
          step.setStatus(toValidationStatus(action.status()));
          LOG.info(
              "Step status validation={} step={} from={} to={}",
              validationKey,
              action.step(),
              previousStatus,
              step.getStatus());
          changed = true;
        }
        case SET_VALIDATION_STATUS -> {
          Validation.Status previousStatus = validation.getStatus();
          validation.setStatus(toValidationStatus(action.status()));
          LOG.info(
              "Workflow status validation={} from={} to={}",
              validationKey,
              previousStatus,
              validation.getStatus());
          changed = true;
        }
        case DISPATCH -> {
          // Dispatch after persisting status updates.
        }
      }
    }

    if (changed) {
      client.update(validation);
      LOG.debug("Persisted workflow state validation={}", validationKey);
    } else {
      LOG.debug(
          "No workflow state changes validation={} event={} step={}",
          validationKey,
          event.kind(),
          event.step());
    }

    for (WorkflowDecision.Action action : actions) {
      if (action.kind() == WorkflowDecision.Kind.DISPATCH) {
        LOG.info("Dispatching step validation={} step={}", validationKey, action.step());
        dispatcher.dispatch(action.step(), requests.get(action.step()));
      }
    }
  }

  private JobContext jobContext(Validation validation, int attempt) {
    return new JobContext(
        validation.getKey(),
        attempt,
        validation.getMetrics().getStepTypes().stream()
            .sorted(java.util.Comparator.comparingInt(ValidationStep::getExecutionOrder))
            .map(step -> StepType.valueOf(step.getStepType()))
            .toList());
  }

  public boolean hasOutgoingDestination(StepType completedStep) {
    return dispatcher.hasOutgoingDestination(completedStep);
  }

  public void publishOutgoing(StepType completedStep, Object message) throws IOException {
    dispatcher.dispatchOutgoing(completedStep, message);
  }

  private WorkflowDecision evaluate(
      WorkflowState state, WorkflowEvent event, List<WorkflowChange> changes) {
    if (!dagProperties.getNodes().isEmpty()) {
      return new DagWorkflowEngine(dagProperties.toDefinition()).evaluate(state, event, changes);
    }

    if (!changes.isEmpty()) {
      throw new IllegalStateException("Workflow changes require configured DAG nodes");
    }

    return sequentialEngine.evaluate(state, event);
  }

  private Validation.Status toValidationStatus(WorkflowState.Status status) {
    return switch (status) {
      case QUEUED -> Validation.Status.QUEUED;
      case RUNNING -> Validation.Status.RUNNING;
      case FINISHED -> Validation.Status.FINISHED;
      case SKIPPED -> throw new IllegalStateException(
          "SKIPPED requires support in validator-ws before persisting DAG decisions");
      case FAILED -> Validation.Status.FAILED;
      case ABORTED -> Validation.Status.ABORTED;
      case PENDING -> throw new IllegalArgumentException("Cannot persist pending as a status");
    };
  }
}
