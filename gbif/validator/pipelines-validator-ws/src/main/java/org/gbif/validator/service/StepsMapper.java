package org.gbif.validator.service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;

@Slf4j
@UtilityClass
public class StepsMapper {

  /** Maps raw step types to ValidationSteps. */
  public static List<ValidationStep> mapToValidationSteps(Set<String> stepTypes) {

    List<ValidationStep> collect =
        stepTypes.stream()
            .map(StepType::valueOf)
            .filter(st -> st != StepType.VALIDATOR_UPLOAD_ARCHIVE)
            .map(
                st ->
                    Metrics.ValidationStep.builder()
                        .executionOrder(PipelinesWorkflow.getValidatorWorkflow().getLevel(st))
                        .stepType(st.name())
                        .build())
            .collect(Collectors.toList());

    collect.add(
        Metrics.ValidationStep.builder()
            .stepType(StepType.VALIDATOR_UPLOAD_ARCHIVE.name())
            .status(Validation.Status.FINISHED)
            .executionOrder(
                PipelinesWorkflow.getValidatorWorkflow()
                    .getLevel(StepType.VALIDATOR_UPLOAD_ARCHIVE))
            .build());

    return collect;
  }

  /**
   * Creates a list with Singleton list containing
   * Metrics.ValidationStep.StepType.VALIDATOR_UPLOAD_ARCHIVE.
   */
  public static List<Metrics.ValidationStep> getUploadingSteps(
      Validation.Status status, String message) {
    return Collections.singletonList(
        Metrics.ValidationStep.builder()
            .stepType(StepType.VALIDATOR_UPLOAD_ARCHIVE.name())
            .status(status)
            .message(message)
            .build());
  }
}
