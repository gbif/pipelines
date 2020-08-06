package org.gbif.pipelines.core.config.factory;

import io.github.resilience4j.retry.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.core.config.model.RetryConfig;

/**
 * Creates the configuration to use a retry service.
 *
 * <p>By default it reads the configuration from the "properties" file.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RetryFactory {

  /** @return a new {@link Retry} instance using the supplied configuration. */
  public static Retry create(@Nullable RetryConfig retryConfig, String name) {
    RetryConfig config = Objects.isNull(retryConfig) ? new RetryConfig() : retryConfig;
    IntervalFunction intervalFn =
        IntervalFunction.ofExponentialRandomBackoff(
            config.getInitialIntervalMillis(),
            config.getMultiplier(),
            config.getRandomizationFactor());
    io.github.resilience4j.retry.RetryConfig resilienceRetryConfig =
        io.github.resilience4j.retry.RetryConfig.custom()
            .maxAttempts(config.getMaxAttempts())
            .intervalFunction(intervalFn)
            .build();
    return Retry.of(name, resilienceRetryConfig);
  }
}
