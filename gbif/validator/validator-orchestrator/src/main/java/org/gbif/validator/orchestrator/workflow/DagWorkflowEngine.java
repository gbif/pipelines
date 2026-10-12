package org.gbif.validator.orchestrator.workflow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.orchestrator.workflow.WorkflowDecision.Action;
import org.gbif.validator.orchestrator.workflow.WorkflowState.Status;
import org.gbif.validator.orchestrator.workflow.WorkflowState.Step;

/** Pure DAG evaluator. Configured nodes must match the persisted workflow snapshot. */
public final class DagWorkflowEngine implements WorkflowEngine {

  private final DagDefinition definition;
  private final Map<DagDefinition.JoinPolicy, JoinEvaluator> evaluators;

  public DagWorkflowEngine(DagDefinition definition) {
    this(definition, Map.of());
  }

  public DagWorkflowEngine(
      DagDefinition definition, Map<DagDefinition.JoinPolicy, JoinEvaluator> overrides) {
    this.definition = Objects.requireNonNull(definition, "definition");
    this.evaluators = new EnumMap<>(DagDefinition.JoinPolicy.class);
    this.evaluators.put(DagDefinition.JoinPolicy.NON_ERROR, this::nonError);
    this.evaluators.put(DagDefinition.JoinPolicy.ALL_TERMINAL, this::allTerminal);
    this.evaluators.putAll(Objects.requireNonNull(overrides, "overrides"));
  }

  @Override
  public WorkflowDecision evaluate(WorkflowState state, WorkflowEvent event) {
    return evaluate(state, event, List.of());
  }

  public WorkflowDecision evaluate(
      WorkflowState state, WorkflowEvent event, List<WorkflowChange> changes) {
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(changes, "changes");
    Objects.requireNonNull(event, "event");

    if (isTerminal(state.status())) {
      return new WorkflowDecision(List.of());
    }

    Map<StepType, Status> statuses = new EnumMap<>(StepType.class);
    for (Step step : state.steps()) {
      statuses.put(step.type(), step.status());
    }
    if (!statuses.keySet().equals(definition.stepTypes())) {
      throw new IllegalArgumentException("DAG definition and workflow steps differ");
    }

    List<Action> actions = new ArrayList<>();
    if (event.kind() != WorkflowEvent.Kind.START) {
      Status current = statuses.get(event.step());
      if (current != Status.QUEUED && current != Status.RUNNING) {
        return new WorkflowDecision(List.of());
      }

      Status outcome = event.kind() == WorkflowEvent.Kind.COMPLETED
          ? Status.FINISHED : Status.FAILED;
      statuses.put(event.step(), outcome);
      actions.add(Action.stepStatus(event.step(), outcome));

    }

    // Step-specific decisions only affect steps that have not started.
    if (event.kind() != WorkflowEvent.Kind.COMPLETED && !changes.isEmpty()) {
      throw new IllegalArgumentException("Workflow changes require a successful completion");
    }

    for (WorkflowChange change : changes) {
      Status previous = statuses.get(change.step());

      if (previous != Status.PENDING) {
        throw new IllegalArgumentException(
            "Cannot change an active or terminal step: " + change.step());
      }

      if (change.kind() == WorkflowChange.Kind.SKIP) {
        statuses.put(change.step(), Status.SKIPPED);
        actions.add(Action.stepStatus(change.step(), Status.SKIPPED));
      }
    }

    // Evaluate deterministic order; avoid dispatching anything while a step remains active
    // when sequential mode is configured.
    List<Step> ordered = state.steps().stream()
        .sorted(Comparator.comparingInt(Step::order).thenComparing(s -> s.type().name()))
        .toList();
    boolean active = statuses.values().stream()
        .anyMatch(status -> status == Status.QUEUED || status == Status.RUNNING);

    // A join may become terminal and unblock another join, so repeat until stable.
    boolean changed;
    do {
      changed = false;
      for (Step step : ordered) {
        if (statuses.get(step.type()) != Status.PENDING) {
          continue;
        }

        DagDefinition.Node node = definition.require(step.type());
        List<Status> predecessors = node.dependencies().stream()
            .map(statuses::get).toList();
        JoinEvaluator evaluator = evaluators.get(definition.policy(node));
        JoinDecision decision = evaluator.evaluate(node, predecessors);

        if (decision == JoinDecision.ABORT || decision == JoinDecision.SKIP) {
          Status terminal = decision == JoinDecision.ABORT ? Status.ABORTED : Status.SKIPPED;
          statuses.put(step.type(), terminal);
          actions.add(Action.stepStatus(step.type(), terminal));
          changed = true;
        }
      }
    } while (changed);

    boolean hasFailure = statuses.values().stream().anyMatch(status -> status == Status.FAILED);
    for (Step step : ordered) {
      if (statuses.get(step.type()) != Status.PENDING || hasFailure) {
        continue;
      }

      DagDefinition.Node node = definition.require(step.type());
      List<Status> predecessors = node.dependencies().stream()
          .map(statuses::get).toList();
      JoinDecision decision = evaluators.get(definition.policy(node)).evaluate(node, predecessors);
      if (decision == JoinDecision.EXECUTE && (definition.parallel() || !active)) {
        statuses.put(step.type(), Status.QUEUED);
        actions.add(Action.stepStatus(step.type(), Status.QUEUED));
        actions.add(Action.validationStatus(Status.RUNNING));
        actions.add(Action.dispatch(step.type()));
        active = true;
      }
    }

    if (statuses.values().stream().allMatch(this::isTerminal)) {
      boolean aborted = statuses.values().stream()
          .anyMatch(status -> status == Status.FAILED || status == Status.ABORTED);
      boolean failed = statuses.values().stream().anyMatch(status -> status == Status.FAILED);
      actions.add(Action.validationStatus(failed ? Status.FAILED
          : aborted ? Status.ABORTED : Status.FINISHED));
    }

    return new WorkflowDecision(actions);
  }

  private JoinDecision nonError(DagDefinition.Node node, List<Status> predecessors) {
    if (predecessors.stream().anyMatch(status -> !isTerminal(status))) {
      return JoinDecision.WAIT;
    }
    if (predecessors.stream().anyMatch(
        status -> status == Status.FAILED || status == Status.ABORTED)) {
      return JoinDecision.ABORT;
    }
    return JoinDecision.EXECUTE;
  }

  private JoinDecision allTerminal(DagDefinition.Node node, List<Status> predecessors) {
    return predecessors.stream().allMatch(this::isTerminal)
        ? JoinDecision.EXECUTE : JoinDecision.WAIT;
  }

  private boolean isTerminal(Status status) {
    return status == Status.FINISHED || status == Status.SKIPPED
        || status == Status.FAILED || status == Status.ABORTED;
  }
}
