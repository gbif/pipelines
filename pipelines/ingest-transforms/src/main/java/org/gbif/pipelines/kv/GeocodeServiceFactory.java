package org.gbif.pipelines.kv;

import java.io.IOException;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.pipelines.parsers.config.model.KvConfig;
import org.gbif.pipelines.parsers.parsers.location.GeocodeService;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.geocode.GeocodeResponse;

import lombok.SneakyThrows;

/**
 * Factory to get singleton instance of {@link GeocodeService}
 */
public class GeocodeServiceFactory {

  private final GeocodeService service;
  private static volatile GeocodeServiceFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private GeocodeServiceFactory(KvConfig config) {
    service = create(config);
  }

  /* TODO Comment */
  public static GeocodeService getInstance(KvConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new GeocodeServiceFactory(config);
        }
      }
    }
    return instance.service;
  }

  /* TODO Comment */
  @SneakyThrows
  public static GeocodeService create(KvConfig config) {
    return GeocodeService.create(creatKvStore(config), BitmapFactory.getInstance(config));
  }

  private static KeyValueStore<LatLng, GeocodeResponse> creatKvStore(KvConfig config) throws IOException {
    if (config == null) {
      return null;
    }

    ClientConfiguration clientConfig = ClientConfiguration.builder()
        .withBaseApiUrl(config.getBasePath()) //GBIF base API url
        .withFileCacheMaxSizeMb(config.getCacheSizeMb()) //Max file cache size
        .withTimeOut(config.getTimeout()) //Geocode service connection time-out
        .build();

    if (config.getZookeeperUrl() == null || config.isRestOnly()) {
      return GeocodeKVStoreFactory.simpleGeocodeKVStore(clientConfig);
    }

    CachedHBaseKVStoreConfiguration geocodeKvStoreConfig = CachedHBaseKVStoreConfiguration.builder()
        .withValueColumnQualifier("j") //stores JSON data
        .withHBaseKVStoreConfiguration(HBaseKVStoreConfiguration.builder()
            .withTableName(config.getTableName()) //Geocode KV HBase table
            .withColumnFamily("v") //Column in which qualifiers are stored
            .withNumOfKeyBuckets(config.getNumOfKeyBuckets()) //Buckets for salted key generations == to # of region servers
            .withHBaseZk(config.getZookeeperUrl()) //HBase Zookeeper ensemble
            .build())
        .withCacheCapacity(15_000L)
        .build();

    return GeocodeKVStoreFactory.simpleGeocodeKVStore(geocodeKvStoreConfig, clientConfig);
  }

}
