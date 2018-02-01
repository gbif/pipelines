package org.gbif.pipelines.core.functions.ws.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

/**
 * Models the ws configuration.
 */
public class Config {

  private static final String WS_BASE_PATH_PROP = "ws.basePath";
  private static final String WS_TIMEOUT_PROP = "ws.timeoutSeconds";

  // defualts
  private static final String DEFAULT_TIMEOUT = "60";
  private static final String DEFAULT_CACHE_SIZE = "100";

  private String basePath;
  private long timeout;
  private CacheConfig cacheConfig;

  public Config() { }

  public Config(String propertiesFilePath) {
    Properties props = new Properties();
    try (
      FileInputStream in = new FileInputStream(getClass().getClassLoader().getResource(propertiesFilePath).getFile())) {
      props.load(in);
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not load properties file " + propertiesFilePath, e);
    }

    this.basePath = Optional.ofNullable(props.getProperty(WS_BASE_PATH_PROP))
      .filter(s -> !s.isEmpty())
      .orElseThrow(() -> new IllegalArgumentException("WS base path is required"));
    this.timeout = Long.parseLong(props.getProperty(WS_TIMEOUT_PROP, DEFAULT_TIMEOUT));

    Optional<String> cacheNameOptional =
      Optional.ofNullable(props.getProperty(CacheConfig.CACHE_NAME_PROP)).filter(s -> !s.isEmpty());

    if (cacheNameOptional.isPresent()) {
      this.cacheConfig = new CacheConfig();
      this.cacheConfig.name = cacheNameOptional.get();
      this.cacheConfig.size =
        Long.parseLong(props.getProperty(CacheConfig.CACHE_SIZE_PROP, DEFAULT_CACHE_SIZE)) * 1024 * 1024;
    }

  }

  public String getBasePath() {
    return basePath;
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
  public class CacheConfig {

    private static final String CACHE_NAME_PROP = "cache.name";
    private static final String CACHE_SIZE_PROP = "cache.sizeInMb";

    private String name;
    // size in bytes
    private long size;

    public String getName() {
      return name;
    }

    public long getSize() {
      return size;
    }

  }

}
