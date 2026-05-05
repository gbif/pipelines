package org.gbif.pipelines.tasks;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.PipelinesWorkflow.Graph;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.tasks.client.RetryingValidationClient;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Validations {

  public static void updateStatus(
      RetryingValidationClient validationClient, UUID key, StepType stepType, Status status) {
    updateStatus(validationClient, key, stepType, status, null);
  }

  public static void updateStatus(
      RetryingValidationClient validationClient,
      UUID key,
      StepType stepType,
      Status status,
      String message) {

    Validation validation = validationClient.get(key);
    if (validation == null) {
      log.warn("Can't find validation data key {}, please check that record exists", key);
      return;
    }

    Graph<StepType> validatorWorkflow = PipelinesWorkflow.getValidatorWorkflow();
    // Mark all previous steps as FINISHED
    validation.getMetrics().getStepTypes().stream()
        .filter(step -> validatorWorkflow.getLevel(stepType) > step.getExecutionOrder())
        .forEach(step -> step.setStatus(Status.FINISHED));

    Status newStatus = status;
    if (validation.hasFinished()) {
      newStatus = validation.getStatus();
    }

    Status mainStatus = newStatus;
    if (mainStatus == Status.FINISHED) {
      boolean isQueued =
          validation.getMetrics().getStepTypes().stream()
              .filter(x -> !x.getStepType().equals(stepType.name()))
              .anyMatch(x -> x.getStatus() != Status.FINISHED);
      if (isQueued) {
        mainStatus = Status.QUEUED;
      }
    }
    validation.setStatus(mainStatus);

    validation.setModified(Timestamp.valueOf(ZonedDateTime.now().toLocalDateTime()));

    Metrics metrics =
        Optional.ofNullable(validation.getMetrics()).orElse(Metrics.builder().build());

    boolean addValidationType = true;
    for (ValidationStep step : metrics.getStepTypes()) {
      // Required to keep validation api separate to gbif-api
      if (step.getStepType().equals(stepType.name())) {
        step.setStatus(newStatus);
        step.setMessage(message);
        addValidationType = false;
        break;
      }
    }

    if (addValidationType) {
      ValidationStep step =
          ValidationStep.builder()
              .stepType(stepType.name())
              .status(newStatus)
              .message(message)
              .executionOrder(validatorWorkflow.getLevel(stepType))
              .build();
      metrics.getStepTypes().add(step);
    }

    validation.setMetrics(metrics);

    log.info(
        "Validation {} main state to {} and step state to {} with message {}",
        stepType,
        mainStatus,
        newStatus,
        message);
    validationClient.update(key, validation);
  }
}
