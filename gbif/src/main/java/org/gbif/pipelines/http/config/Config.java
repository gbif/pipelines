package org.gbif.pipelines.http.config;

/**
 * Models the http configuration.
 */
public class Config {

  private String basePath;
  private long timeout;
  private CacheConfig cacheConfig;

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public void setCacheConfig(CacheConfig cacheConfig) {
    this.cacheConfig = cacheConfig;
  }

  public long getTimeout() {
    return timeout;
  }

  public CacheConfig getCacheConfig() {
    return cacheConfig;
  }

  /**
   * Models the cache configuration.
   */
  public static class CacheConfig {

    private String name;
    // size in bytes
    private long size;

    public String getName() {
      return name;
    }

    public long getSize() {
      return size;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setSize(long size) {
      this.size = size;
    }
  }

}
