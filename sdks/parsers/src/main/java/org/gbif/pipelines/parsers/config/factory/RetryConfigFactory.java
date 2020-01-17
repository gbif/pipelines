package org.gbif.pipelines.parsers.config.factory;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

import org.gbif.pipelines.parsers.config.model.PipelinesRetryConfig;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Creates the configuration for retry services.
 *
 * <p>By default it reads the configuration from the "properties" file.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RetryConfigFactory {

  private static final int DEFAULT_MAX_ATTEMPTS = 3;
  private static final long DEFAULT_INITIAL_INTERVAL = 500;
  private static final double DEFAULT_MULTIPLIER = 1.5;
  private static final double DEFAULT_RANDOMIZATION_FACTOR = 0.5;

  // property suffixes
  private static final String MAX_ATTEMPTS_PROP = "retry.max_attempts";
  private static final String INITIAL_INTERVALS_PROP = "retry.initial_interval";
  private static final String MULTIPLIER_PROP = "retry.multiplier";
  private static final String RANDOMIZATION_FACTOR_PROP = "retry.randomization_factor";

  /**@return a RetryConfig with default values*/
  public static PipelinesRetryConfig create() {
    return PipelinesRetryConfig.create(DEFAULT_MAX_ATTEMPTS, DEFAULT_INITIAL_INTERVAL, DEFAULT_MULTIPLIER, DEFAULT_RANDOMIZATION_FACTOR);
  }

  /**@return a RetryConfig from the properties settings*/
  public static PipelinesRetryConfig create(@NonNull Path propertiesPath, @NonNull String prefix) {
    // load properties or throw exception if cannot be loaded
    Properties props = ConfigFactory.loadProperties(propertiesPath);

    return create(props, prefix);
  }

  public static PipelinesRetryConfig create(@NonNull Properties props, @NonNull String prefix) {
    int maxAttempts = Optional.ofNullable(props.getProperty(prefix + MAX_ATTEMPTS_PROP)).map(Integer::parseInt).orElse(DEFAULT_MAX_ATTEMPTS);
    long initialInterval = Optional.ofNullable(props.getProperty(prefix + INITIAL_INTERVALS_PROP)).map(Long::parseLong).orElse(DEFAULT_INITIAL_INTERVAL);
    double multiplier = Optional.ofNullable(props.getProperty(prefix + MULTIPLIER_PROP)).map(Double::parseDouble).orElse( DEFAULT_MULTIPLIER);
    double randomizationFactor = Optional.ofNullable(props.getProperty(prefix + RANDOMIZATION_FACTOR_PROP)).map(Double::parseDouble).orElse(DEFAULT_RANDOMIZATION_FACTOR);
    return PipelinesRetryConfig.create(maxAttempts, initialInterval, multiplier, randomizationFactor);
  }
}
