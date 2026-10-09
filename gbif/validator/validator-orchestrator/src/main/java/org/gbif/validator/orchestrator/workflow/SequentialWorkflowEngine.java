package org.gbif.validator.orchestrator.workflow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.gbif.validator.orchestrator.workflow.WorkflowDecision.Action;
import org.gbif.validator.orchestrator.workflow.WorkflowState.Status;
import org.gbif.validator.orchestrator.workflow.WorkflowState.Step;

/** Strict sequential transitions; never skips an incomplete or blocked step. */
public final class SequentialWorkflowEngine implements WorkflowEngine {
  @Override
  public WorkflowDecision evaluate(WorkflowState state, WorkflowEvent event) {
    Objects.requireNonNull(state);
    Objects.requireNonNull(event);
    if (state.status() == Status.ABORTED || state.status() == Status.FAILED
        || state.status() == Status.FINISHED) {
      return new WorkflowDecision(List.of());
    }


    List<Step> steps = state.steps().stream()
        .sorted(Comparator.comparingInt(Step::order)).toList();
    Step firstIncomplete = steps.stream().filter(s -> s.status() != Status.FINISHED)
        .findFirst().orElse(null);
    List<Action> actions = new ArrayList<>();

    if (event.kind() == WorkflowEvent.Kind.START) {
      if (firstIncomplete == null || firstIncomplete.status() != Status.PENDING) {
        return new WorkflowDecision(List.of());
      }

      queue(actions, firstIncomplete);
      return new WorkflowDecision(actions);
    }


    // A completion must belong to the current active step; stale or unexpected events do nothing.
    if (firstIncomplete == null || firstIncomplete.type() != event.step()
        || (firstIncomplete.status() != Status.QUEUED
            && firstIncomplete.status() != Status.RUNNING)) {
      return new WorkflowDecision(List.of());
    }

    if (event.kind() == WorkflowEvent.Kind.FAILED) {
      actions.add(Action.stepStatus(firstIncomplete.type(), Status.FAILED));
      actions.add(Action.validationStatus(Status.FAILED));
      return new WorkflowDecision(actions);
    }

    actions.add(Action.stepStatus(firstIncomplete.type(), Status.FINISHED));
    int index = steps.indexOf(firstIncomplete);
    if (index == steps.size() - 1) {
      actions.add(Action.validationStatus(Status.FINISHED));
    } else if (steps.get(index + 1).status() == Status.PENDING) {
      queue(actions, steps.get(index + 1));
    } else {
      // Inconsistent snapshot: never finish or dispatch through a blocked step.
      throw new IllegalStateException("Next step is not pending: " + steps.get(index + 1).type());
    }
    return new WorkflowDecision(actions);
  }

  private void queue(List<Action> actions, Step step) {
    actions.add(Action.stepStatus(step.type(), Status.QUEUED));
    actions.add(Action.validationStatus(Status.RUNNING));
    actions.add(Action.dispatch(step.type()));
  }
}
