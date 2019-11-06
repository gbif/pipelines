package org.gbif.pipelines.parsers.config;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/** Models the exponential backoff configuration. If you want to create an instance, use {@link RetryConfigFactory} */
@Getter
@Data
@AllArgsConstructor
public class RetryConfig implements Serializable {

  private static final long serialVersionUID = 4099754042526608429L;

  //Maximum number of attempts
  private final Integer maxAttempts;

  //Initial interval after first failure
  private final Long initialIntervalMillis;

  //Multiplier factor after each retry
  private final Double multiplier;

  //Random factor to add between each retry
  private final Double randomizationFactor;
}
