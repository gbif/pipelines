package org.gbif.validator.orchestrator.workflow;

import java.util.List;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;

/** Ordered actions whose side effects are executed outside the workflow engine. */
public final class WorkflowDecision {

  public enum Kind {
    SET_STEP_STATUS,
    SET_VALIDATION_STATUS,
    DISPATCH
  }

  public static final class Action {

    private final Kind kind;
    private final StepType step;
    private final WorkflowState.Status status;

    private Action(Kind kind, StepType step, WorkflowState.Status status) {
      this.kind = kind;
      this.step = step;
      this.status = status;
    }

    public static Action stepStatus(StepType step, WorkflowState.Status status) {
      return new Action(
          Kind.SET_STEP_STATUS, Objects.requireNonNull(step), Objects.requireNonNull(status));
    }

    public static Action validationStatus(WorkflowState.Status status) {
      return new Action(Kind.SET_VALIDATION_STATUS, null, Objects.requireNonNull(status));
    }

    public static Action dispatch(StepType step) {
      return new Action(Kind.DISPATCH, Objects.requireNonNull(step), null);
    }

    public Kind kind() {
      return kind;
    }

    public StepType step() {
      return step;
    }

    public WorkflowState.Status status() {
      return status;
    }
  }

  private final List<Action> actions;

  public WorkflowDecision(List<Action> actions) {
    this.actions = List.copyOf(actions);
  }

  public List<Action> actions() {
    return actions;
  }

  public boolean isNoop() {
    return actions.isEmpty();
  }
}
