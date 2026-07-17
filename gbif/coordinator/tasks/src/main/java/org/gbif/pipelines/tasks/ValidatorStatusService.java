/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.tasks;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.PipelinesWorkflow.Graph;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.tasks.client.RetryingValidationClient;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;

/**
 * Manages validation status updates in the validation service for a dataset being processed by the
 * validator workflow. Unlike the regular pipeline tracking, this service writes directly to the
 * validation API rather than the pipelines history service.
 *
 * <p>It is responsible for:
 *
 * <ul>
 *   <li>Checking whether a validation has reached a terminal or non-resumable state.
 *   <li>Updating the per-step status in {@link org.gbif.validator.api.Metrics} and deriving the
 *       overall {@link Validation} status from step completion.
 *   <li>Marking all preceding steps (lower execution order) as {@link Status#FINISHED} whenever a
 *       later step is reported, keeping the step list consistent.
 * </ul>
 */
@Slf4j
@RequiredArgsConstructor
public class ValidatorStatusService {

  private final RetryingValidationClient validationClient;

  /**
   * Returns {@code true} when the validation is in a state that should prevent further processing:
   * {@link Status#ABORTED}, {@link Status#FINISHED}, {@link Status#FAILED}, or {@link
   * Status#DOWNLOADING}. Returns {@code false} when no validation record exists (treated as
   * not-yet-started rather than aborted).
   */
  public boolean isValidatorAborted(PipelineBasedMessage message) {
    Validation validation = validationClient.get(message.getDatasetUuid());
    if (validation == null) {
      log.warn(
          "Can't find validation data key {}, please check that record exists",
          message.getDatasetUuid());
      return false;
    }

    Status status = validation.getStatus();
    return status == Status.ABORTED
        || status == Status.FINISHED
        || status == Status.FAILED
        || status == Status.DOWNLOADING;
  }

  /**
   * Equivalent to {@link #updateStatus(PipelineBasedMessage, StepType, Status, String)} with no
   * message text.
   */
  public void updateStatus(PipelineBasedMessage message, StepType stepType, Status status) {
    updateStatus(message, stepType, status, null);
  }

  /**
   * Updates the status of {@code stepType} to {@code status} and recalculates the overall
   * validation status. If the validation has already reached a finished state, the step status is
   * locked to that terminal status rather than the requested one. The overall status becomes {@link
   * Status#FINISHED} only when all other steps are also finished; otherwise it remains {@link
   * Status#QUEUED}.
   */
  public void updateStatus(
      PipelineBasedMessage message, StepType stepType, Status status, String text) {
    Validation validation = validationClient.get(message.getDatasetUuid());
    if (validation == null) {
      log.warn(
          "Can't find validation data key {}, please check that record exists",
          message.getDatasetUuid());
      return;
    }

    Graph<StepType> validatorWorkflow = PipelinesWorkflow.getValidatorWorkflow();

    validation.getMetrics().getStepTypes().stream()
        .filter(step -> validatorWorkflow.getLevel(stepType) > step.getExecutionOrder())
        .forEach(step -> step.setStatus(Status.FINISHED));

    Status stepStatus = status;
    if (validation.hasFinished()) {
      stepStatus = validation.getStatus();
    }

    Status mainStatus = resolveMainStatus(validation, stepType, stepStatus);
    validation.setStatus(mainStatus);
    validation.setModified(Timestamp.valueOf(ZonedDateTime.now().toLocalDateTime()));

    Metrics metrics =
        Optional.ofNullable(validation.getMetrics()).orElse(Metrics.builder().build());
    updateStepStatus(metrics, validatorWorkflow, stepType, stepStatus, text);
    validation.setMetrics(metrics);

    log.info(
        "Validation {} main state to {} and step state to {} with message {}",
        stepType,
        mainStatus,
        stepStatus,
        text);

    validationClient.update(message.getDatasetUuid(), validation);
  }

  private Status resolveMainStatus(Validation validation, StepType stepType, Status stepStatus) {
    if (stepStatus != Status.FINISHED) {
      return stepStatus;
    }

    boolean hasUnfinishedSteps =
        validation.getMetrics().getStepTypes().stream()
            .filter(step -> !step.getStepType().equals(stepType.name()))
            .anyMatch(step -> step.getStatus() != Status.FINISHED);

    return hasUnfinishedSteps ? Status.QUEUED : Status.FINISHED;
  }

  private void updateStepStatus(
      Metrics metrics,
      Graph<StepType> validatorWorkflow,
      StepType stepType,
      Status stepStatus,
      String text) {
    for (Metrics.ValidationStep step : metrics.getStepTypes()) {
      if (step.getStepType().equals(stepType.name())) {
        step.setStatus(stepStatus);
        step.setMessage(text);
        return;
      }
    }

    Metrics.ValidationStep step =
        Metrics.ValidationStep.builder()
            .stepType(stepType.name())
            .status(stepStatus)
            .message(text)
            .executionOrder(validatorWorkflow.getLevel(stepType))
            .build();

    metrics.getStepTypes().add(step);
  }
}
