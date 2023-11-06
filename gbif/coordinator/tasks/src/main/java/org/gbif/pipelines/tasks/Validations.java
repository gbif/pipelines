package org.gbif.pipelines.tasks;

import io.github.resilience4j.retry.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.PipelinesWorkflow.Graph;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Validations {

  private static final Retry RETRY =
      Retry.of(
          "validatorCall",
          RetryConfig.custom()
              .maxAttempts(7)
              .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(6)))
              .build());

  public static void updateStatus(
      ValidationWsClient validationClient, UUID key, StepType stepType, Status status) {

    Validation validation = Retry.decorateFunction(RETRY, validationClient::get).apply(key);
    if (validation == null) {
      log.warn("Can't find validation data key {}, please check that record exists", key);
      return;
    }

    Graph<StepType> validatorWorkflow = PipelinesWorkflow.getValidatorWorkflow();
    // Mark all previous steps as FINISHED
    for (ValidationStep step : validation.getMetrics().getStepTypes()) {
      if (validatorWorkflow.getLevel(stepType) > step.getExecutionOrder()) {
        step.setStatus(Status.FINISHED);
      }
    }

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
        addValidationType = false;
        break;
      }
    }

    if (addValidationType) {
      ValidationStep step =
          ValidationStep.builder()
              .stepType(stepType.name())
              .status(newStatus)
              .executionOrder(validatorWorkflow.getLevel(stepType))
              .build();
      metrics.getStepTypes().add(step);
    }

    validation.setMetrics(metrics);

    log.info("Validaton {} main state to {} and step state to {}", stepType, mainStatus, newStatus);
    Retry.decorateRunnable(RETRY, () -> validationClient.update(key, validation)).run();
  }
}
