package org.gbif.pipelines.parsers.config.factory;

import java.util.Objects;

import org.gbif.pipelines.parsers.config.model.PipelinesRetryConfig;

import io.github.resilience4j.retry.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Creates the configuration to use a retry service.
 *
 * <p>By default it reads the configuration from the "properties" file.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RetryFactory {

  /**
   * @return a new {@link Retry} instance using the supplied configuration.
   */
  public static Retry create(@Nullable PipelinesRetryConfig pipelinesRetryConfig, String name) {
    PipelinesRetryConfig config = Objects.isNull(pipelinesRetryConfig) ? RetryConfigFactory.create() : pipelinesRetryConfig;
    IntervalFunction intervalFn = IntervalFunction.ofExponentialRandomBackoff(config.getInitialIntervalMillis(),
        config.getMultiplier(),
        config.getRandomizationFactor());
    RetryConfig resilienceRetryConfig = RetryConfig.custom()
        .maxAttempts(config.getMaxAttempts())
        .intervalFunction(intervalFn)
        .build();
    return Retry.of(name, resilienceRetryConfig);
  }
}
