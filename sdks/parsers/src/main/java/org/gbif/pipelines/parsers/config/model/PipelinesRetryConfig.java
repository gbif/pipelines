package org.gbif.pipelines.parsers.config.model;

import java.io.Serializable;

import org.gbif.pipelines.parsers.config.factory.RetryConfigFactory;

import lombok.Data;

/**
 * Models the exponential backoff configuration. If you want to create an instance, use {@link RetryConfigFactory}
 */
@Data(staticConstructor = "create")
public class PipelinesRetryConfig implements Serializable {

  private static final long serialVersionUID = -8983292173694266924L;

  //Maximum number of attempts
  private final Integer maxAttempts;

  //Initial interval after first failure
  private final Long initialIntervalMillis;

  //Multiplier factor after each retry
  private final Double multiplier;

  //Random factor to add between each retry
  private final Double randomizationFactor;
}
