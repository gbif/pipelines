package org.gbif.validator.orchestrator.service;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.orchestrator.workflow.SequentialWorkflowEngine;
import org.gbif.validator.orchestrator.workflow.ValidationWorkflowMapper;
import org.gbif.validator.orchestrator.workflow.WorkflowDecision;
import org.gbif.validator.orchestrator.workflow.WorkflowEvent;
import org.gbif.validator.orchestrator.workflow.WorkflowState;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.springframework.stereotype.Service;

/**
 * Connects the pure engine to validator-ws and a message dispatcher.
 * Not a safe concurrent/atomic execution gate: validator-ws exposes full-object PUT,
 * not conditional status transitions, and RabbitMQ publishes are not in that transaction.
 * Callers must serialize events per validation and arrange recovery for publish failures.
 */
@Service
public class OrchestrationService {
  private final ValidationWsClient client;
  private final StepDispatcher dispatcher;
  private final List<StepRequestFactory> factories;
  private final ValidationWorkflowMapper mapper = new ValidationWorkflowMapper();
  private final SequentialWorkflowEngine engine = new SequentialWorkflowEngine();

  public OrchestrationService(ValidationWsClient client, StepDispatcher dispatcher,
      List<StepRequestFactory> factories) {
    this.client = Objects.requireNonNull(client);
    this.dispatcher = Objects.requireNonNull(dispatcher);
    this.factories = List.copyOf(factories);
  }

  /** Side-effect-free plan based on the latest server snapshot. */
  public WorkflowDecision plan(UUID validationKey, WorkflowEvent event) {
    return engine.evaluate(mapper.from(client.get(validationKey)), event);
  }

  /**
   * Applies a plan by saving status changes before dispatch. Not invoked by an MQ
   * listener until conditional transitions and a recovery strategy are added.
   */
  public void handle(UUID validationKey, WorkflowEvent event) throws IOException {
    Validation validation = client.get(validationKey);
    WorkflowDecision decision = engine.evaluate(mapper.from(validation), event);

    // Resolve payloads up-front to avoid changing state for unsupported steps.
    Map<StepType, Object> requests = new EnumMap<>(StepType.class);
    for (WorkflowDecision.Action action : decision.actions()) {
      if (action.kind() == WorkflowDecision.Kind.DISPATCH) {
        StepRequestFactory factory = factories.stream().filter(f -> f.supports(action.step()))
            .findFirst().orElseThrow(() -> new IllegalStateException(
                "No message factory for step " + action.step()));
        requests.put(action.step(), factory.create(action.step(), validation));
      }
    }

    boolean changed = false;
    for (WorkflowDecision.Action action : decision.actions()) {
      switch (action.kind()) {
        case SET_STEP_STATUS -> {
          ValidationStep step = validation.getMetrics().getStepTypes().stream()
              .filter(s -> s.getStepType().equals(action.step().name())).findFirst()
              .orElseThrow();
          step.setStatus(toValidationStatus(action.status()));
          changed = true;
        }
        case SET_VALIDATION_STATUS -> {
          validation.setStatus(toValidationStatus(action.status()));
          changed = true;
        }
        case DISPATCH -> {
          // Dispatch after persisting status updates.
        }
      }
    }

    if (changed) {
      client.update(validation);
    }

    for (WorkflowDecision.Action action : decision.actions()) {
      if (action.kind() == WorkflowDecision.Kind.DISPATCH) {
        dispatcher.dispatch(action.step(), requests.get(action.step()));
      }
    }
  }

  private Validation.Status toValidationStatus(WorkflowState.Status status) {
    return switch (status) {
      case QUEUED -> Validation.Status.QUEUED;
      case RUNNING -> Validation.Status.RUNNING;
      case FINISHED -> Validation.Status.FINISHED;
      case FAILED -> Validation.Status.FAILED;
      case ABORTED -> Validation.Status.ABORTED;
      case PENDING -> throw new IllegalArgumentException("Cannot persist pending as a status");
    };
  }
}
