package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RetryConfig implements Serializable {

  private static final long serialVersionUID = -8983292173694266924L;

  // Maximum number of attempts
  private Integer maxAttempts = 3;

  // Initial interval after first failure
  private Long initialIntervalMillis = 500L;

  // Multiplier factor after each retry
  private Double multiplier = 1.5d;

  // Random factor to add between each retry
  private Double randomizationFactor = 0.5d;
}
