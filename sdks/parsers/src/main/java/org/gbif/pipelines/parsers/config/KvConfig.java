package org.gbif.pipelines.parsers.config;

import java.io.Serializable;

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

  public KvConfig(String basePath, long timeout, long cacheSizeMb, String zookeeperUrl, int numOfKeyBuckets, String tableName) {
    this.basePath = basePath;
    this.timeout = timeout;
    this.cacheSizeMb = cacheSizeMb;
    this.zookeeperUrl = zookeeperUrl;
    this.numOfKeyBuckets = numOfKeyBuckets;
    this.tableName = tableName;
  }

  public String getBasePath() {
    return basePath;
  }

  public long getTimeout() {
    return timeout;
  }

  public long getCacheSizeMb() {
    return cacheSizeMb;
  }

  public String getZookeeperUrl() {
    return zookeeperUrl;
  }

  public int getNumOfKeyBuckets() {
    return numOfKeyBuckets;
  }

  public String getTableName() {
    return tableName;
  }
}
