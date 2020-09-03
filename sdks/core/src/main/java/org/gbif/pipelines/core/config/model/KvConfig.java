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

  private long wsTimeoutSec = 60L;

  private long wsCacheSizeMb = 64L;

  private int numOfKeyBuckets;

  private String tableName;

  private boolean restOnly = false;
}
