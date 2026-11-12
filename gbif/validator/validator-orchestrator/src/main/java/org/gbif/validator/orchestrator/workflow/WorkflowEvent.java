package org.gbif.validator.orchestrator.workflow;

import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;

public final class WorkflowEvent {

  public enum Kind {
    START,
    COMPLETED,
    FAILED
  }

  private final Kind kind;
  private final StepType step;

  private WorkflowEvent(Kind kind, StepType step) {
    this.kind = kind;
    this.step = step;
  }

  public static WorkflowEvent start() {
    return new WorkflowEvent(Kind.START, null);
  }

  public static WorkflowEvent completed(StepType step) {
    return new WorkflowEvent(Kind.COMPLETED, Objects.requireNonNull(step));
  }

  public static WorkflowEvent failed(StepType step) {
    return new WorkflowEvent(Kind.FAILED, Objects.requireNonNull(step));
  }

  public Kind kind() {
    return kind;
  }

  public StepType step() {
    return step;
  }
}
