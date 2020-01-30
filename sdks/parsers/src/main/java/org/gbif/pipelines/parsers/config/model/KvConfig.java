package org.gbif.pipelines.parsers.config.model;

import java.io.Serializable;

import lombok.Data;

@Data(staticConstructor = "create")
public final class KvConfig implements Serializable {

  private static final long serialVersionUID = -9019714539959567270L;
  // ws path
  private final String basePath;
  // timeout in seconds
  private final long timeout;
  // cache size in mb
  private final long cacheSizeMb;
  //
  private final String tableName;

  private final String zookeeperUrl;

  private final int numOfKeyBuckets;

  private final boolean restOnly;

  private final String imagePath;
}

