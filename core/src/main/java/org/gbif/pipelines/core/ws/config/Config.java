package org.gbif.pipelines.core.ws.config;

import java.io.Serializable;
import java.util.Objects;

/** Models the ws configuration. */
public final class Config implements Serializable {

  // ws path
  private final String basePath;
  // timeout in seconds
  private final long timeout;
  // cache size in bytes
  private final long cacheSize;

  private Config(Builder builder) {
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
   * Package-private to force the creation of {@link Config} instances using the {@link
   * HttpConfigFactory}.
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

    public Config build() {
      return new Config(this);
    }
  }
}
