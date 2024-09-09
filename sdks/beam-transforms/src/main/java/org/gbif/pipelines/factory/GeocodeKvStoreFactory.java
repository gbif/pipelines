package org.gbif.pipelines.factory;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Optional;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration.Builder;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.LoaderRetryConfig;
import org.gbif.pipelines.core.config.model.KvConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.parsers.location.GeocodeKvStore;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.geocode.GeocodeResponse;

/** Factory to get singleton instance of {@link KeyValueStore} */
public class GeocodeKvStoreFactory {

  private final KeyValueStore<LatLng, GeocodeResponse> geocodeKvStore;
  private static volatile GeocodeKvStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private GeocodeKvStoreFactory(HdfsConfigs hdfsConfigs, PipelinesConfig config) {
    BufferedImage image =
        Optional.ofNullable(config.getImageCachePath())
            .map(ip -> BufferedImageFactory.getInstance(hdfsConfigs, ip))
            .orElse(null);
    KeyValueStore<LatLng, GeocodeResponse> kvStore = creatKvStore(config);
    geocodeKvStore = GeocodeKvStore.create(kvStore, image);
  }

  public static KeyValueStore<LatLng, GeocodeResponse> getInstance(
      HdfsConfigs hdfsConfigs, PipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new GeocodeKvStoreFactory(hdfsConfigs, config);
        }
      }
    }
    return instance.geocodeKvStore;
  }

  public static SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> createSupplier(
      HdfsConfigs hdfsConfigs, PipelinesConfig config) {
    return () -> new GeocodeKvStoreFactory(hdfsConfigs, config).geocodeKvStore;
  }

  public static SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> getInstanceSupplier(
      HdfsConfigs hdfsConfigs, PipelinesConfig config) {
    return () -> GeocodeKvStoreFactory.getInstance(hdfsConfigs, config);
  }

  private static KeyValueStore<LatLng, GeocodeResponse> creatKvStore(PipelinesConfig config)
      throws IOException {
    if (config == null) {
      return null;
    }

    KvConfig geocodeConfig = config.getGeocode();

    String api =
        Optional.ofNullable(geocodeConfig.getApi())
            .map(WsConfig::getWsUrl)
            .orElse(config.getGbifApi().getWsUrl());

    ClientConfiguration clientConfig =
        ClientConfiguration.builder()
            .withBaseApiUrl(api)
            .withFileCacheMaxSizeMb(geocodeConfig.getWsCacheSizeMb())
            .withTimeOut(geocodeConfig.getWsTimeoutSec())
            .build();

    String zk = geocodeConfig.getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? config.getZkConnectionString() : zk;
    if (zk == null || geocodeConfig.isRestOnly()) {
      return GeocodeKVStoreFactory.simpleGeocodeKVStore(clientConfig);
    }

    Builder configBuilder =
        CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("j") // stores JSON data
            .withHBaseKVStoreConfiguration(
                HBaseKVStoreConfiguration.builder()
                    .withTableName(geocodeConfig.getTableName())
                    .withColumnFamily("v") // Column in which qualifiers are stored
                    .withNumOfKeyBuckets(geocodeConfig.getNumOfKeyBuckets())
                    .withHBaseZk(zk)
                    .withHBaseZnode(geocodeConfig.getHbaseZnode())
                    .build())
            .withCacheCapacity(25_000L)
            .withCacheExpiryTimeInSeconds(geocodeConfig.getCacheExpiryTimeInSeconds());

    KvConfig.LoaderRetryConfig retryConfig = geocodeConfig.getLoaderRetryConfig();
    if (retryConfig != null) {
      configBuilder.withLoaderRetryConfig(
          new LoaderRetryConfig(
              retryConfig.getMaxAttempts(),
              retryConfig.getInitialIntervalMillis(),
              retryConfig.getMultiplier(),
              retryConfig.getRandomizationFactor()));
    }

    return GeocodeKVStoreFactory.simpleGeocodeKVStore(configBuilder.build(), clientConfig);
  }
}
