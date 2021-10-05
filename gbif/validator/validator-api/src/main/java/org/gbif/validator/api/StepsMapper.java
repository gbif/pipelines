package org.gbif.validator.api;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class StepsMapper {

  /** Maps raw step types to ValidationSteps. */
  public static List<Metrics.ValidationStep> mapToValidationSteps(Set<String> stepTypes) {

    List<Metrics.ValidationStep> collect =
        stepTypes.stream()
            .map(Metrics.ValidationStep.StepType::valueOf)
            .filter(st -> st != Metrics.ValidationStep.StepType.VALIDATOR_UPLOAD_ARCHIVE)
            .map(
                st ->
                    Metrics.ValidationStep.builder()
                        .executionOrder(st.getExecutionOrder())
                        .stepType(st)
                        .build())
            .collect(Collectors.toList());

    collect.add(
        Metrics.ValidationStep.builder()
            .stepType(Metrics.ValidationStep.StepType.VALIDATOR_UPLOAD_ARCHIVE)
            .status(Validation.Status.FINISHED)
            .executionOrder(
                Metrics.ValidationStep.StepType.VALIDATOR_UPLOAD_ARCHIVE.getExecutionOrder())
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
            .stepType(Metrics.ValidationStep.StepType.VALIDATOR_UPLOAD_ARCHIVE)
            .status(status)
            .message(message)
            .build());
  }
}
