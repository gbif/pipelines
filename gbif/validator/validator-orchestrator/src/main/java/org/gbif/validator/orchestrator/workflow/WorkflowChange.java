package org.gbif.validator.orchestrator.workflow;

import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;

/** Request to exclude an as-yet-unstarted step from the active DAG. */
public record WorkflowChange(StepType step, Kind kind) {

  public enum Kind {
    SKIP
  }

  public WorkflowChange {
    Objects.requireNonNull(step, "step");
    Objects.requireNonNull(kind, "kind");
  }

  public static WorkflowChange skip(StepType step) {
    return new WorkflowChange(step, Kind.SKIP);
  }
}
