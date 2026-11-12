package org.gbif.validator.orchestrator.workflow;

import java.util.List;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;
import org.gbif.validator.orchestrator.workflow.WorkflowState.Status;

/** Converts validator-ws persistence DTOs to the pure workflow snapshot. */
public final class ValidationWorkflowMapper {
  public WorkflowState from(Validation validation) {
    if (validation == null
        || validation.getMetrics() == null
        || validation.getMetrics().getStepTypes() == null) {
      throw new IllegalArgumentException("Validation has no persisted workflow");
    }

    List<WorkflowState.Step> steps =
        validation.getMetrics().getStepTypes().stream()
            .map(
                s ->
                    new WorkflowState.Step(
                        StepType.valueOf(s.getStepType()),
                        s.getExecutionOrder(),
                        status(s.getStatus())))
            .toList();
    return new WorkflowState(status(validation.getStatus()), steps);
  }

  private Status status(Validation.Status status) {
    if (status == null) {
      return Status.PENDING;
    }

    return switch (status) {
      case QUEUED -> Status.QUEUED;
      case RUNNING -> Status.RUNNING;
      case FINISHED -> Status.FINISHED;
      case FAILED -> Status.FAILED;
      case ABORTED -> Status.ABORTED;
      default -> Status.PENDING;
    };
  }
}
