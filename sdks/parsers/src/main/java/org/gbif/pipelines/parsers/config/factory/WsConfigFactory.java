package org.gbif.pipelines.parsers.config.factory;

import java.nio.file.Path;
import java.util.Properties;

import org.gbif.pipelines.parsers.config.model.PipelinesRetryConfig;
import org.gbif.pipelines.parsers.config.model.WsConfig;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Creates the configuration to use a specific WS.
 *
 * <p>By default it reads the configuration from the "http.properties" file.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class WsConfigFactory {

  public static final String METADATA_PREFIX = "metadata";
  public static final String BLAST_PREFIX = "blast";

  // property suffixes
  private static final String WS_BASE_PATH_PROP = "gbif.api.url";
  private static final String WS_TIMEOUT_PROP = ".ws.timeout";
  private static final String CACHE_SIZE_PROP = ".ws.cache.sizeMb";
  private static final String URL_PROP = ".ws.url";

  // property defaults
  private static final String DEFAULT_TIMEOUT = "60";
  private static final String DEFAULT_CACHE_SIZE_MB = "64";

  public static WsConfig create(@NonNull Path propertiesPath, @NonNull String prefix) {
    // load properties or throw exception if cannot be loaded
    Properties props = ConfigFactory.loadProperties(propertiesPath);

    return create(props, prefix);
  }

  /** Creates a {@link WsConfig} from a url and uses default timeout and cache size. */
  public static WsConfig create(String url) {
    return create(url, DEFAULT_TIMEOUT, DEFAULT_CACHE_SIZE_MB);
  }

  /** Creates a {@link WsConfig} from a url and uses default timeout and cache size. */
  public static WsConfig create(String url, String timeout, String cacheSize) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "url is required");
    return new WsConfig(url, timeout, cacheSize, RetryConfigFactory.create());
  }

  /** Creates a {@link WsConfig} from a url and uses default timeout and cache size. */
  public static WsConfig create(String url, long timeout, long cacheSize) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "url is required");
    return new WsConfig(url, timeout, cacheSize, RetryConfigFactory.create());
  }

  public static WsConfig create(@NonNull Properties props, @NonNull String prefix) {
    // get the base path or throw exception if not present
    String url = props.getProperty(prefix + URL_PROP);
    if (Strings.isNullOrEmpty(url)) {
      url = props.getProperty(WS_BASE_PATH_PROP);
      if (Strings.isNullOrEmpty(url)) {
        throw new IllegalArgumentException("WS base path is required");
      }
    }

    String cacheSize = props.getProperty(prefix + CACHE_SIZE_PROP, DEFAULT_CACHE_SIZE_MB);
    String timeout = props.getProperty(prefix + WS_TIMEOUT_PROP, DEFAULT_TIMEOUT);
    PipelinesRetryConfig pipelinesRetryConfig = RetryConfigFactory.create(props, prefix);

    return new WsConfig(url, timeout, cacheSize, pipelinesRetryConfig);
  }
}
