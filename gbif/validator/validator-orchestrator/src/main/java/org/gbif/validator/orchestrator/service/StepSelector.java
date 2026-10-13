package org.gbif.validator.orchestrator.service;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.springframework.stereotype.Service;

/** Selects the first unfinished step from persisted validator-ws workflow state. */
@Service
public class StepSelector {
  public Optional<StepType> nextPendingStep(Validation validation) {
    Objects.requireNonNull(validation, "validation must not be null");
    if (validation.getMetrics() == null || validation.getMetrics().getStepTypes() == null) {
      throw new IllegalArgumentException("Validation is missing persisted workflow steps");
    }
    if (validation.getStatus() == Validation.Status.ABORTED
        || validation.getStatus() == Validation.Status.FAILED
        || validation.getStatus() == Validation.Status.FINISHED) {
      return Optional.empty();
    }
    Optional<ValidationStep> firstIncomplete =
        validation.getMetrics().getStepTypes().stream()
            .sorted(Comparator.comparingInt(ValidationStep::getExecutionOrder))
            .filter(step -> step.getStatus() != Validation.Status.FINISHED)
            .findFirst();
    if (firstIncomplete.isEmpty()) {
      return Optional.empty();
    }
    ValidationStep step = firstIncomplete.get();
    // Never dispatch already queued/running work or skip an aborted/failed step.
    if (step.getStatus() != null) {
      return Optional.empty();
    }
    return Optional.of(StepType.valueOf(step.getStepType()));
  }
}
