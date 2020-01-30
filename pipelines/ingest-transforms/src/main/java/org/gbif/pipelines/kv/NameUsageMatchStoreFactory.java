package org.gbif.pipelines.kv;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.parsers.config.model.KvConfig;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatch;

import lombok.SneakyThrows;

/**
 * Factory to get singleton instance of KV store {@link KeyValueStore}
 */
public class NameUsageMatchStoreFactory {

  private final KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore;
  private static volatile NameUsageMatchStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private NameUsageMatchStoreFactory(KvConfig config) {
    this.kvStore = create(config);
  }

  /* TODO Comment */
  public static KeyValueStore<SpeciesMatchRequest, NameUsageMatch> getInstance(KvConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new NameUsageMatchStoreFactory(config);
        }
      }
    }
    return instance.kvStore;
  }

  /* TODO Comment */
  @SneakyThrows
  public static KeyValueStore<SpeciesMatchRequest, NameUsageMatch> create(KvConfig config) {
    if (config == null) {
      return null;
    }

    ClientConfiguration clientConfiguration = ClientConfiguration.builder()
        .withBaseApiUrl(config.getBasePath()) //GBIF base API url
        .withFileCacheMaxSizeMb(config.getCacheSizeMb()) //Max file cache size
        .withTimeOut(config.getTimeout()) //Geocode service connection time-out
        .build();

    if (config.getZookeeperUrl() == null || config.isRestOnly()) {
      return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(clientConfiguration);
    }

    CachedHBaseKVStoreConfiguration matchConfig = CachedHBaseKVStoreConfiguration.builder()
        .withValueColumnQualifier("j") //stores JSON data
        .withHBaseKVStoreConfiguration(HBaseKVStoreConfiguration.builder()
            .withTableName(config.getTableName()) //Geocode KV HBase table
            .withColumnFamily("v") //Column in which qualifiers are stored
            .withNumOfKeyBuckets(config.getNumOfKeyBuckets()) //Buckets for salted key generations
            .withHBaseZk(config.getZookeeperUrl()) //HBase Zookeeper ensemble
            .build())
        .withCacheCapacity(15_000L)
        .build();

    return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(matchConfig, clientConfiguration);
  }

}
