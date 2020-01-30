package org.gbif.pipelines.parsers.config.factory;

import java.nio.file.Path;
import java.util.Properties;

import org.gbif.pipelines.parsers.config.model.KvConfig;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
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
  private static final String TABLE_NAME = ".tableName";
  private static final String REST_ONLY_NAME = ".restOnly";

  // property defaults
  private static final String DEFAULT_TIMEOUT_SEC = "60";
  private static final String DEFAULT_CACHE_SIZE_MB = "64";
  private static final String DEFAULT_NUM_OF_KEY_BUCKETS = "10";
  private static final Boolean DEFAULT_REST_ONLY = Boolean.FALSE;

  public static final String BITMAP_PROPERTY_NAME = GEOCODE_PREFIX + ".bitmapPath";
  public static final String BITMAP_FILE_NAME = "bitmap/bitmap.png";

  public static KvConfig create(@NonNull Path propertiesPath, @NonNull String prefix) {
    // load properties or throw exception if cannot be loaded
    Properties props = ConfigFactory.loadProperties(propertiesPath);
    return create(props, prefix);
  }

  public static KvConfig create(@NonNull Properties props, @NonNull String prefix) {
    // get the base path or throw exception if not present
    String basePath = ConfigFactory.getKey(props, WS_BASE_PATH_PROP) + "/v1/";
    String zookeeperUrl = props.getProperty(ZOOKEEPER_PROP);
    String tableName = props.getProperty(prefix + TABLE_NAME);
    String imagePath = props.getProperty(BITMAP_PROPERTY_NAME, BITMAP_FILE_NAME);

    boolean restOnly = Boolean.parseBoolean(props.getProperty(prefix + REST_ONLY_NAME, DEFAULT_REST_ONLY.toString()));
    long cacheSize = Long.parseLong(props.getProperty(prefix + CACHE_SIZE_PROP, DEFAULT_CACHE_SIZE_MB));
    long timeout = Long.parseLong(props.getProperty(prefix + WS_TIMEOUT_PROP, DEFAULT_TIMEOUT_SEC));
    int numOfKeyBuckets = Integer.parseInt(props.getProperty(prefix + NUM_OF_KEY_BUCKETS, DEFAULT_NUM_OF_KEY_BUCKETS));

    return KvConfig.create(basePath, timeout, cacheSize, tableName, zookeeperUrl, numOfKeyBuckets, restOnly, imagePath);
  }

}
