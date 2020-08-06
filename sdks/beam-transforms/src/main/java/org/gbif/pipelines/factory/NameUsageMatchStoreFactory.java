package org.gbif.pipelines.factory;

import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatch;

/** Factory to get singleton instance of KV store {@link KeyValueStore} */
public class NameUsageMatchStoreFactory {

  private final KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore;
  private static volatile NameUsageMatchStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private NameUsageMatchStoreFactory(PipelinesConfig config) {
    this.kvStore = create(config);
  }

  /* TODO Comment */
  public static KeyValueStore<SpeciesMatchRequest, NameUsageMatch> getInstance(
      PipelinesConfig config) {
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
  public static KeyValueStore<SpeciesMatchRequest, NameUsageMatch> create(PipelinesConfig config) {
    if (config == null) {
      return null;
    }

    ClientConfiguration clientConfiguration =
        ClientConfiguration.builder()
            .withBaseApiUrl(config.getGbifApi().getWsUrl())
            .withFileCacheMaxSizeMb(config.getNameUsageMatch().getWsCacheSizeMb())
            .withTimeOut(config.getNameUsageMatch().getWsTimeoutSec())
            .build();

    String zk = config.getNameUsageMatch().getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? config.getZkConnectionString() : zk;
    if (zk == null || config.getNameUsageMatch().isRestOnly()) {
      return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(clientConfiguration);
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
                    .build())
            .withCacheCapacity(15_000L)
            .build();

    return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(matchConfig, clientConfiguration);
  }

  public static SerializableSupplier<KeyValueStore<SpeciesMatchRequest, NameUsageMatch>>
      createSupplier(PipelinesConfig config) {
    return () -> NameUsageMatchStoreFactory.create(config);
  }

  public static SerializableSupplier<KeyValueStore<SpeciesMatchRequest, NameUsageMatch>>
      getInstanceSupplier(PipelinesConfig config) {
    return () -> NameUsageMatchStoreFactory.getInstance(config);
  }
}
