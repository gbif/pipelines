package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KvConfig implements Serializable {

  private static final long serialVersionUID = 9165679151024130462L;

  /** List of Zookeeper servers to connect to */
  private String zkConnectionString;

  private String hbaseZnode;

  private long wsTimeoutSec = 60L;

  private long wsCacheSizeMb = 64L;

  private int numOfKeyBuckets;

  private String tableName;

  private boolean restOnly = false;

  private WsConfig api;

  private long cacheExpiryTimeInSeconds = 300L;

  private LoaderRetryConfig loaderRetryConfig;

  @Data
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LoaderRetryConfig implements Serializable {

    private static final long serialVersionUID = 9165679151024245962L;

    private Integer maxAttempts = 3;

    private Long initialIntervalMillis = 1_000L;

    private Double multiplier = 1.5d;

    private Double randomizationFactor = 0.5d;
  }
}
