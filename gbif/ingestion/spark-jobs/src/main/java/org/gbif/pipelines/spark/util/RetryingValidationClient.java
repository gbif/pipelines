package org.gbif.pipelines.spark.util;

import com.fasterxml.jackson.core.JsonParseException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.validator.api.Validation;

@Slf4j
@RequiredArgsConstructor
public class RetryingValidationClient {

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

  private final ValidationClient validationClient;

  public Validation get(UUID key) {
    return Retry.decorateFunction(RETRY, this::getWithoutRetry).apply(key);
  }

  private Validation getWithoutRetry(UUID key) {
    log.info("Validation client: get validation by datasetKey {}", key);
    return validationClient.get(key);
  }

  public void update(UUID key, Validation validation) {
    Retry.decorateRunnable(RETRY, () -> updateWithoutRetry(key, validation)).run();
  }

  private void updateWithoutRetry(UUID key, Validation validation) {
    log.info("Validation client: update validation by datasetKey {}", key);
    validationClient.update(key, validation);
  }
}
