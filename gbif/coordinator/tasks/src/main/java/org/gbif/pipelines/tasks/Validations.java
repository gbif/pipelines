package org.gbif.pipelines.tasks;

import com.fasterxml.jackson.core.JsonParseException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
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
              .maxAttempts(15)
              .retryExceptions(JsonParseException.class, IOException.class, TimeoutException.class)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      Duration.ofSeconds(1), 2d, Duration.ofSeconds(30)))
              .build());

  public static void updateStatus(
      ValidationWsClient validationClient, UUID key, StepType stepType, Status status) {
    updateStatus(validationClient, key, stepType, status, null);
  }

  public static void updateStatus(
      ValidationWsClient validationClient,
      UUID key,
      StepType stepType,
      Status status,
      String message) {

    Validation validation = Retry.decorateFunction(RETRY, validationClient::get).apply(key);
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
        "Validaton {} main state to {} and step state to {} with message {}",
        stepType,
        mainStatus,
        newStatus,
        message);
    Retry.decorateRunnable(RETRY, () -> validationClient.update(key, validation)).run();
  }
}
