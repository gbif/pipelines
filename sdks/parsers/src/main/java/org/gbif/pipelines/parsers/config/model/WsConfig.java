package org.gbif.pipelines.parsers.config.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
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

  // http headers to add to each request
  private Map<String, String> httpHeaders = Collections.emptyMap();

  // Retry configuration
  private RetryConfig retryConfig = new RetryConfig();
}
