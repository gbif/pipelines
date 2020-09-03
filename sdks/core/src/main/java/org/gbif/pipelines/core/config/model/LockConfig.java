package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Pipeline Options to create Zookeeper shared locks using a ExponentialBackoffRetry strategy to
 * connect.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class LockConfig implements Serializable {

  private static final long serialVersionUID = 7222736128801212446L;

  /** List of Zookeeper servers to connect to */
  private String zkConnectionString;

  /** Zookeeper shared path or space */
  private String namespace;

  /** Base locking path path to use for locking */
  private String lockingPath;

  /** Shared-lock name */
  private String lockName;

  /** Initial amount of time to wait between retries */
  private int sleepTimeMs = 100;

  /** Max number of times to retry */
  private int maxRetries = 5;
}
