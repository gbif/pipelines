package org.gbif.pipelines.parsers.config;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/**
 * Pipeline Options to create Zookeeper shared locks using a ExponentialBackoffRetry strategy to connect.
 */
@Getter
@Data
@AllArgsConstructor(staticName = "create")
public class LockConfig implements Serializable {

  private static final long serialVersionUID = 7222736128801212446L;

  /** List of Zookeeper servers to connect to */
  private final String zkConnectionString;

  /** Zookeeper shared path or space */
  private final String namespace;

  /** Base locking path path to use for locking */
  private final String lockingPath;

  /** Shared-lock name */
  private final String lockName;

  /** Initial amount of time to wait between retries */
  private final int sleepTimeMs;

  /** Max number of times to retry */
  private final int maxRetries;

}
