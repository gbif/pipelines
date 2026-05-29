package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SamplingConfig implements Serializable {

  private static final long serialVersionUID = -9019714539955270671L;

  // ws path
  private String wsUrl;

  // timeout in seconds
  private long timeoutSec = 60L;

  // cache size in bytes
  private long cacheSizeMb = 64L;

  // sampling batch size
  private int batchSize = 25000;

  // sleep time between requesting batches
  private int batchStatusSleepTime = 1000;

  // retries
  private int downloadRetries = 5;

  // http headers to add to each request
  private Map<String, String> httpHeaders = Collections.emptyMap();

  // Retry configuration
  private RetryConfig retryConfig = new RetryConfig();
}
