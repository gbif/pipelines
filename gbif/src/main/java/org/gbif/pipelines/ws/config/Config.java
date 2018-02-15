package org.gbif.pipelines.ws.config;

/**
 * Models the ws configuration.
 */
public class Config {

  // property names
  private static final String WS_BASE_PATH_PROP_SUFFIX = ".ws.basePath";
  private static final String WS_TIMEOUT_PROP_SUFFIX = ".ws.timeoutSeconds";

  // defaults
  private static final String DEFAULT_TIMEOUT = "60";
  private static final String DEFAULT_CACHE_SIZE = "100";

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

    private static final String CACHE_NAME_PROP = ".cache.name";
    private static final String CACHE_SIZE_PROP = ".cache.sizeInMb";

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
