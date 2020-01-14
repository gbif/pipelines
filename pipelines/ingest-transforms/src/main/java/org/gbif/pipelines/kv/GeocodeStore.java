package org.gbif.pipelines.kv;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.geocode.GeocodeResponse;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

/**
 * Create KV store for geocode lookup using KV store {@link KvConfig}
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GeocodeStore {

  @SneakyThrows
  public static KeyValueStore<LatLng, GeocodeResponse> create(KvConfig config) {
    if (config != null) {
      ClientConfiguration clientConfig = ClientConfiguration.builder()
          .withBaseApiUrl(config.getBasePath()) //GBIF base API url
          .withFileCacheMaxSizeMb(config.getCacheSizeMb()) //Max file cache size
          .withTimeOut(config.getTimeout()) //Geocode service connection time-out
          .build();

      if (config.getZookeeperUrl() != null && !config.isRestOnly()) {

        CachedHBaseKVStoreConfiguration geocodeKvStoreConfig = CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("j") //stores JSON data
            .withHBaseKVStoreConfiguration(HBaseKVStoreConfiguration.builder()
                .withTableName(config.getTableName()) //Geocode KV HBase table
                .withColumnFamily("v") //Column in which qualifiers are stored
                .withNumOfKeyBuckets(
                    config.getNumOfKeyBuckets()) //Buckets for salted key generations == to # of region servers
                .withHBaseZk(config.getZookeeperUrl()) //HBase Zookeeper ensemble
                .build())
            .withCacheCapacity(15_000L)
            .build();

        return GeocodeKVStoreFactory.simpleGeocodeKVStore(geocodeKvStoreConfig, clientConfig);
      } else {
        return GeocodeKVStoreFactory.simpleGeocodeKVStore(clientConfig);
      }
    }
    return null;
  }

}
