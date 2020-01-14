package org.gbif.pipelines.kv;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatch;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NameUsageMatchStore {

  @SneakyThrows
  public static KeyValueStore<SpeciesMatchRequest, NameUsageMatch> get(KvConfig config) {
    if (config != null) {
      ClientConfiguration clientConfiguration = ClientConfiguration.builder()
          .withBaseApiUrl(config.getBasePath()) //GBIF base API url
          .withFileCacheMaxSizeMb(config.getCacheSizeMb()) //Max file cache size
          .withTimeOut(config.getTimeout()) //Geocode service connection time-out
          .build();

      if (config.getZookeeperUrl() != null && !config.isRestOnly()) {

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
      } else {
        return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(clientConfiguration);
      }
    }
    return null;
  }

}
