package org.gbif.pipelines.parsers.ws.config;

import java.io.Serializable;

/** Models the ws configuration. If you want to create an istance, use {@link WsConfigFactory} */
public final class WsConfig implements Serializable {

  private static final long serialVersionUID = -9019714539955270670L;
  // ws path
  private final String basePath;
  // timeout in seconds
  private final long timeout;
  // cache size in bytes
  private final long cacheSize;

  public WsConfig(String basePath, long timeout, long cacheSize) {
    this.basePath = basePath;
    this.timeout = timeout;
    this.cacheSize = cacheSize;
  }

  public WsConfig(String basePath, String timeout, String cacheSizeMb) {
    this.basePath = basePath;
    this.timeout = Long.parseLong(timeout);
    this.cacheSize = Long.parseLong(cacheSizeMb) * 1024L * 1024L;
  }

  public String getBasePath() {
    return basePath;
  }

  public long getTimeout() {
    return timeout;
  }

  public long getCacheSize() {
    return cacheSize;
  }
}
