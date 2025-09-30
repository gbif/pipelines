package org.gbif.pipelines.factory;

import java.io.IOException;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration.Builder;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.LoaderRetryConfig;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.config.model.ChecklistKvConfig;
import org.gbif.pipelines.core.config.model.KvConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/** Factory to get singleton instance of KV store {@link KeyValueStore} */
@Slf4j
public class NameUsageMatchStoreFactory {

  private final KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore;
  private static volatile NameUsageMatchStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private NameUsageMatchStoreFactory(PipelinesConfig config) {
    this.kvStore = createMultipleService(config);
  }

  /* TODO Comment */
  public static KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> getInstance(
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

  @SneakyThrows
  public static KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> createMultipleService(
      PipelinesConfig config) {

    if (config == null) {
      log.error("PipelinesConfig is null - unable to create KV store for NameUsageMatch");
      return null;
    }

    if (config.getNameUsageMatchingService() == null
        || config.getNameUsageMatchingService().getChecklistKeys() == null
        || config.getNameUsageMatchingService().getChecklistKeys().isEmpty()) {
      log.error("No checklist keys specified - unable to create KV store for NameUsageMatch");
      return null;
    }

    ChecklistKvConfig checklistKvConfig = config.getNameUsageMatchingService();
    return constructKV(checklistKvConfig.getWs(), checklistKvConfig.getWs().getApi().getWsUrl());
  }

  private static KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> constructKV(
      KvConfig kvConfig, String gbifApi) {
    String api = Optional.ofNullable(kvConfig.getApi()).map(WsConfig::getWsUrl).orElse(gbifApi);

    ClientConfiguration clientConfiguration =
        ClientConfiguration.builder()
            .withBaseApiUrl(api)
            .withFileCacheMaxSizeMb(kvConfig.getWsCacheSizeMb())
            .withTimeOutMillisec(kvConfig.getWsTimeoutSec() * 1000)
            .withMaxConnections(kvConfig.getMaxConnections())
            .build();

    String zk = kvConfig.getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? kvConfig.getZkConnectionString() : zk;
    if (zk == null || kvConfig.isRestOnly()) {
      log.error("ZK config is null - wont use HBase, only REST API");
      return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(clientConfiguration);
    }

    Builder matchConfigBuilder =
        CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("j") // stores JSON data
            .withHBaseKVStoreConfiguration(
                HBaseKVStoreConfiguration.builder()
                    .withTableName(kvConfig.getTableName())
                    .withColumnFamily("v") // Column in which qualifiers are stored
                    .withNumOfKeyBuckets(kvConfig.getNumOfKeyBuckets())
                    .withHBaseZk(zk)
                    .withHBaseZnode(kvConfig.getHbaseZnode())
                    .build())
            .withCacheCapacity(25_000L)
            .withCacheExpiryTimeInSeconds(kvConfig.getCacheExpiryTimeInSeconds());

    KvConfig.LoaderRetryConfig retryConfig = kvConfig.getLoaderRetryConfig();
    if (retryConfig != null) {
      matchConfigBuilder.withLoaderRetryConfig(
          new LoaderRetryConfig(
              retryConfig.getMaxAttempts(),
              retryConfig.getInitialIntervalMillis(),
              retryConfig.getMultiplier(),
              retryConfig.getRandomizationFactor()));
    }

    try {
      return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(
          matchConfigBuilder.build(), clientConfiguration);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  public static SerializableSupplier<KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>>
      createMultiServiceSupplier(PipelinesConfig config) {
    return () -> createMultipleService(config);
  }
}
