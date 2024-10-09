package org.gbif.pipelines.factory;

import java.util.Optional;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration.Builder;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.LoaderRetryConfig;
import org.gbif.kvs.species.Identification;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.pipelines.core.config.model.KvConfig;
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

    KvConfig nameUsageMatchConfig = config.getNameUsageMatch();

    String api =
        Optional.ofNullable(nameUsageMatchConfig.getApi())
            .map(WsConfig::getWsUrl)
            .orElse(config.getGbifApi().getWsUrl());

    ChecklistbankClientsConfiguration clientConfiguration =
        ChecklistbankClientsConfiguration.builder()
            .nameUsageClientConfiguration(
                ClientConfiguration.builder()
                    .withBaseApiUrl(api)
                    .withFileCacheMaxSizeMb(nameUsageMatchConfig.getWsCacheSizeMb())
                    .withTimeOut(nameUsageMatchConfig.getWsTimeoutSec())
                    .build())
            .checklistbankClientConfiguration(
                ClientConfiguration.builder()
                    .withBaseApiUrl(api)
                    .withFileCacheMaxSizeMb(nameUsageMatchConfig.getWsCacheSizeMb())
                    .withTimeOut(nameUsageMatchConfig.getWsTimeoutSec())
                    .build())
            .build();

    String zk = nameUsageMatchConfig.getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? config.getZkConnectionString() : zk;
    if (zk == null || nameUsageMatchConfig.isRestOnly()) {
      return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(
          clientConfiguration, config.getNameUsageIdMapping());
    }

    Builder configBuilder =
        CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("j") // stores JSON data
            .withHBaseKVStoreConfiguration(
                HBaseKVStoreConfiguration.builder()
                    .withTableName(nameUsageMatchConfig.getTableName())
                    .withColumnFamily("v") // Column in which qualifiers are stored
                    .withNumOfKeyBuckets(nameUsageMatchConfig.getNumOfKeyBuckets())
                    .withHBaseZk(zk)
                    .withHBaseZnode(nameUsageMatchConfig.getHbaseZnode())
                    .build())
            .withCacheCapacity(25_000L)
            .withCacheExpiryTimeInSeconds(nameUsageMatchConfig.getCacheExpiryTimeInSeconds());

    KvConfig.LoaderRetryConfig retryConfig = nameUsageMatchConfig.getLoaderRetryConfig();
    if (retryConfig != null) {
      configBuilder.withLoaderRetryConfig(
          new LoaderRetryConfig(
              retryConfig.getMaxAttempts(),
              retryConfig.getInitialIntervalMillis(),
              retryConfig.getMultiplier(),
              retryConfig.getRandomizationFactor()));
    }

    return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(
        configBuilder.build(), clientConfiguration, config.getNameUsageIdMapping());
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
