package org.gbif.pipelines.parsers.ws.config;

import java.io.Serializable;
import java.util.Objects;

/** Models the ws configuration. If you want to create an istance, use {@link WsConfigFactory} */
public final class WsConfig implements Serializable {

  // ws path
  private final String basePath;
  // timeout in seconds
  private final long timeout;
  // cache size in bytes
  private final long cacheSize;

  private WsConfig(Builder builder) {
    this.basePath = builder.basePath;
    this.timeout = builder.timeout;
    this.cacheSize = builder.cacheSize;
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

  /**
   * Package-private to force the creation of {@link WsConfig} instances using the {@link
   * WsConfigFactory}.
   */
  static class Builder {

    private String basePath;
    private long timeout;
    private long cacheSize;

    Builder basePath(String basePath) {
      Objects.requireNonNull(basePath);
      this.basePath = basePath;
      return this;
    }

    Builder timeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    Builder cacheSize(long cacheSize) {
      this.cacheSize = cacheSize;
      return this;
    }

    public WsConfig build() {
      return new WsConfig(this);
    }
  }
}
