package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public final class WsConfig implements Serializable {

  private static final long serialVersionUID = -9019714539955270670L;

  // ws path
  private String wsUrl;

  // timeout in seconds
  private long timeoutSec = 60L;

  // cache size in bytes
  private long cacheSizeMb = 64L;

  // Retry configuration
  private RetryConfig retryConfig = new RetryConfig();
}
