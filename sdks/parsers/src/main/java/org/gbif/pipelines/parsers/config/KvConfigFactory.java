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

  public static final String TAXONOMY_PREFIX = "taxonomy";
  public static final String GEOCODE_PREFIX = "geocode";
  public static final String AUSTRALIA_PREFIX = "australia.spatial";

  // property suffixes
  private static final String WS_BASE_PATH_PROP = "gbif.api.url";
  private static final String ZOOKEEPER_PROP = "zookeeper.url";
  private static final String WS_TIMEOUT_PROP = ".ws.timeout";
  private static final String CACHE_SIZE_PROP = ".ws.cache.sizeMb";
  private static final String NUM_OF_KEY_BUCKETS = ".numOfKeyBuckets";
  private static final String TABLE_NAME= ".tableName";

  // property defaults
  private static final String DEFAULT_TIMEOUT_SEC = "60";
  private static final String DEFAULT_CACHE_SIZE_MB = "64";
  private static final String DEFAULT_NUM_OF_KEY_BUCKETS = "10";

  private KvConfigFactory() {}

  public static KvConfig create(String keyPrefix, Path propertiesPath) {
    Objects.requireNonNull(keyPrefix);
    Objects.requireNonNull(propertiesPath);
    // load properties or throw exception if cannot be loaded
    Properties props = loadProperties(propertiesPath);

    // get the base path or throw exception if not present
    String basePath =
        Optional.ofNullable(props.getProperty(WS_BASE_PATH_PROP))
            .filter(prop -> !prop.isEmpty())
            .map(value -> value + "/v1/")
            .orElseThrow(() -> new IllegalArgumentException(keyPrefix + " WS base path is required"));

    String zookeeperUrl =
        Optional.ofNullable(props.getProperty(ZOOKEEPER_PROP))
            .filter(prop -> !prop.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException(keyPrefix + " Zookeeper path is required"));

    String tableName =
        Optional.ofNullable(props.getProperty(keyPrefix + TABLE_NAME))
            .filter(prop -> !prop.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException(keyPrefix + " KV table name is required"));

    long cacheSize = Long.valueOf(props.getProperty(keyPrefix + CACHE_SIZE_PROP, DEFAULT_CACHE_SIZE_MB));
    long timeout = Long.valueOf(props.getProperty(keyPrefix + WS_TIMEOUT_PROP, DEFAULT_TIMEOUT_SEC));
    int numOfKeyBuckets = Integer.valueOf(props.getProperty(keyPrefix + NUM_OF_KEY_BUCKETS, DEFAULT_NUM_OF_KEY_BUCKETS));

    return new KvConfig(basePath, timeout, cacheSize, zookeeperUrl, numOfKeyBuckets, tableName);
  }

  public static KvConfig create(String baseApiPath, String zookeeperUrl, int numOfKeyBuckets, String tableName) {
    long timeoutInSec = Long.valueOf(DEFAULT_TIMEOUT_SEC);
    long cacheInMb = Long.valueOf(DEFAULT_CACHE_SIZE_MB);
    return new KvConfig(baseApiPath, timeoutInSec, cacheInMb, zookeeperUrl, numOfKeyBuckets, tableName);
  }

  public static KvConfig create(String baseApiPath, int numOfKeyBuckets, String tableName) {
    long timeoutInSec = Long.valueOf(DEFAULT_TIMEOUT_SEC);
    long cacheInMb = Long.valueOf(DEFAULT_CACHE_SIZE_MB);
    return new KvConfig(baseApiPath, timeoutInSec, cacheInMb, null, numOfKeyBuckets, tableName);
  }

  public static KvConfig create(String baseApiPath, long timeoutInSec, long cacheInMb, String zookeeperUrl, int numOfKeyBuckets, String tableName) {
    return new KvConfig(baseApiPath, timeoutInSec, cacheInMb, zookeeperUrl, numOfKeyBuckets, tableName);
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
