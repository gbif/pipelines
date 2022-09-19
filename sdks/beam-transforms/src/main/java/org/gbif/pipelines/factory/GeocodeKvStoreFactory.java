package org.gbif.pipelines.factory;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Optional;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
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

    String api =
        Optional.ofNullable(config.getGeocode().getApi())
            .map(WsConfig::getWsUrl)
            .orElse(config.getGbifApi().getWsUrl());

    ClientConfiguration clientConfig =
        ClientConfiguration.builder()
            .withBaseApiUrl(api)
            .withFileCacheMaxSizeMb(config.getGeocode().getWsCacheSizeMb())
            .withTimeOut(config.getGeocode().getWsTimeoutSec())
            .build();

    String zk = config.getGeocode().getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? config.getZkConnectionString() : zk;
    if (zk == null || config.getGeocode().isRestOnly()) {
      return GeocodeKVStoreFactory.simpleGeocodeKVStore(clientConfig);
    }

    CachedHBaseKVStoreConfiguration geocodeKvStoreConfig =
        CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("j") // stores JSON data
            .withHBaseKVStoreConfiguration(
                HBaseKVStoreConfiguration.builder()
                    .withTableName(config.getGeocode().getTableName())
                    .withColumnFamily("v") // Column in which qualifiers are stored
                    .withNumOfKeyBuckets(config.getGeocode().getNumOfKeyBuckets())
                    .withHBaseZk(zk)
                    .build())
            .withCacheCapacity(15_000L)
            .withCacheExpiryTimeInSeconds(config.getGeocode().getCacheExpiryTimeInSeconds())
            .build();

    return GeocodeKVStoreFactory.simpleGeocodeKVStore(geocodeKvStoreConfig, clientConfig);
  }
}
