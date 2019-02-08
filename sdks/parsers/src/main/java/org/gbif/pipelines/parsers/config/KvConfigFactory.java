package org.gbif.pipelines.parsers.config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import org.gbif.pipelines.parsers.exception.IORuntimeException;

public class KvConfigFactory {

  // property suffixes
  private static final String WS_BASE_PATH_PROP = "gbif.api.url";
  private static final String WS_TIMEOUT_PROP = "ws.timeout";
  private static final String CACHE_SIZE_PROP = "ws.cache.sizeMb";
  private static final String ZOOKEEPER_PROP = "zookeeper.url";

  // property defaults
  private static final String DEFAULT_TIMEOUT_SEC = "60";
  private static final String DEFAULT_CACHE_SIZE_MB = "64";

  private KvConfigFactory() {}

  public static KvConfig create(Path propertiesPath) {
    Objects.requireNonNull(propertiesPath);
    // load properties or throw exception if cannot be loaded
    Properties props = loadProperties(propertiesPath);

    // get the base path or throw exception if not present
    String basePath =
        Optional.ofNullable(props.getProperty(WS_BASE_PATH_PROP))
            .filter(prop -> !prop.isEmpty())
            .map(value -> value + "/v1/")
            .orElseThrow(() -> new IllegalArgumentException("WS base path is required"));

    String zookeeperUrl =
        Optional.ofNullable(props.getProperty(ZOOKEEPER_PROP))
            .filter(prop -> !prop.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException("ws base path is required"));

    long cacheSize = Long.valueOf(props.getProperty(CACHE_SIZE_PROP, DEFAULT_CACHE_SIZE_MB));
    long timeout = Long.valueOf(props.getProperty(WS_TIMEOUT_PROP, DEFAULT_TIMEOUT_SEC));

    return new KvConfig(basePath, timeout, cacheSize, zookeeperUrl);
  }

  public static KvConfig create(String basePath, String zookeeperUrl) {
    return new KvConfig(basePath, Long.valueOf(DEFAULT_TIMEOUT_SEC), Long.valueOf(DEFAULT_CACHE_SIZE_MB), zookeeperUrl);
  }

  public static KvConfig create(String basePath, long timeoutInSec, long cacheInMb, String zookeeperUrl) {
    return new KvConfig(basePath, timeoutInSec, cacheInMb, zookeeperUrl);
  }

  /**
   *
   */
  private static Properties loadProperties(Path propertiesPath) {
    Function<Path, InputStream> absolute = path -> {
      try {
        return new FileInputStream(path.toFile());
      } catch (Exception ex) {
        String msg = "Properties with absolute path could not be read from " + propertiesPath;
        throw new IORuntimeException(msg, ex);
      }
    };

    Function<Path, InputStream> resource =
        path -> Thread.currentThread().getContextClassLoader().getResourceAsStream(path.toString());

    Function<Path, InputStream> function = propertiesPath.isAbsolute() ? absolute : resource;

    Properties props = new Properties();
    try (InputStream in = function.apply(propertiesPath)) {
      // read properties from input stream
      props.load(in);
    } catch (Exception ex) {
      String msg = "Properties with absolute path could not be read from " + propertiesPath;
      throw new IORuntimeException(msg, ex);
    }

    return props;
  }

}
