package org.gbif.pipelines.factory;

import java.util.Optional;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.species.Identification;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.rest.client.configuration.ChecklistbankClientsConfiguration;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatch;

/** Factory to get singleton instance of KV store {@link KeyValueStore} */
public class NameUsageMatchStoreFactory {

  private final KeyValueStore<Identification, NameUsageMatch> kvStore;
  private static volatile NameUsageMatchStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private NameUsageMatchStoreFactory(PipelinesConfig config) {
    this.kvStore = create(config);
  }

  /* TODO Comment */
  public static KeyValueStore<Identification, NameUsageMatch> getInstance(PipelinesConfig config) {
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
  public static KeyValueStore<Identification, NameUsageMatch> create(PipelinesConfig config) {
    if (config == null) {
      return null;
    }

    String api =
        Optional.ofNullable(config.getNameUsageMatch().getApi())
            .map(WsConfig::getWsUrl)
            .orElse(config.getGbifApi().getWsUrl());

    ChecklistbankClientsConfiguration clientConfiguration =
        ChecklistbankClientsConfiguration.builder()
            .nameUsageClientConfiguration(
                ClientConfiguration.builder()
                    .withBaseApiUrl(api)
                    .withFileCacheMaxSizeMb(config.getNameUsageMatch().getWsCacheSizeMb())
                    .withTimeOut(config.getNameUsageMatch().getWsTimeoutSec())
                    .build())
            .checklistbankClientConfiguration(
                ClientConfiguration.builder()
                    .withBaseApiUrl(api)
                    .withFileCacheMaxSizeMb(config.getNameUsageMatch().getWsCacheSizeMb())
                    .withTimeOut(config.getNameUsageMatch().getWsTimeoutSec())
                    .build())
            .build();

    String zk = config.getNameUsageMatch().getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? config.getZkConnectionString() : zk;
    if (zk == null || config.getNameUsageMatch().isRestOnly()) {
      return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(
          clientConfiguration, config.getNameUsageIdMapping());
    }

    CachedHBaseKVStoreConfiguration matchConfig =
        CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("j") // stores JSON data
            .withHBaseKVStoreConfiguration(
                HBaseKVStoreConfiguration.builder()
                    .withTableName(config.getNameUsageMatch().getTableName())
                    .withColumnFamily("v") // Column in which qualifiers are stored
                    .withNumOfKeyBuckets(config.getNameUsageMatch().getNumOfKeyBuckets())
                    .withHBaseZk(zk)
                    .withHBaseZnode(config.getNameUsageMatch().getHbaseZnode())
                    .build())
            .withCacheCapacity(15_000L)
            .withCacheExpiryTimeInSeconds(config.getNameUsageMatch().getCacheExpiryTimeInSeconds())
            .build();

    return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(
        matchConfig, clientConfiguration, config.getNameUsageIdMapping());
  }

  public static SerializableSupplier<KeyValueStore<Identification, NameUsageMatch>> createSupplier(
      PipelinesConfig config) {
    return () -> NameUsageMatchStoreFactory.create(config);
  }

  public static SerializableSupplier<KeyValueStore<Identification, NameUsageMatch>>
      getInstanceSupplier(PipelinesConfig config) {
    return () -> NameUsageMatchStoreFactory.getInstance(config);
  }
}
