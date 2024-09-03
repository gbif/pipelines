package org.gbif.pipelines.factory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.config.model.KvConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/** Factory to get singleton instance of KV store {@link KeyValueStore} */
public class NameUsageMatchStoreFactory {

  private final KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore;
  private static volatile NameUsageMatchStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private NameUsageMatchStoreFactory(PipelinesConfig config) {
    this.kvStore = create(config);
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
  public static Map<String, KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>>
      createMultipleServices(PipelinesConfig config) {
    if (config == null) {
      return null;
    }

    if (config.getNameUsageMatchServices() == null
        || config.getNameUsageMatchServices().isEmpty()) {
      return null;
    }

    // preserve order of the services
    return config.getNameUsageMatchServices().stream()
        .collect(
            LinkedHashMap::new,
            (map, item) -> {
              KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore =
                  constructKV(item.getWs(), config.getGbifApi().getWsUrl());
              map.put(item.getDatasetKey(), kvStore);
            },
            Map::putAll);
  }

  @SneakyThrows
  public static KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> create(
      PipelinesConfig config) {
    if (config == null) {
      return null;
    }

    if (config.getNameUsageMatch() == null) {
      return null;
    }

    KvConfig kvConfig = config.getNameUsageMatch();
    String gbifApi = config.getGbifApi().getWsUrl();
    return constructKV(kvConfig, gbifApi);
  }

  private static KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> constructKV(
      KvConfig kvConfig, String gbifApi) {
    String api = Optional.ofNullable(kvConfig.getApi()).map(WsConfig::getWsUrl).orElse(gbifApi);

    ClientConfiguration clientConfiguration =
        ClientConfiguration.builder()
            .withBaseApiUrl(api)
            .withFileCacheMaxSizeMb(kvConfig.getWsCacheSizeMb())
            .withTimeOut(kvConfig.getWsTimeoutSec() * 1000)
            .build();

    String zk = kvConfig.getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? kvConfig.getZkConnectionString() : zk;
    if (zk == null || kvConfig.isRestOnly()) {
      return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(clientConfiguration);
    }

    CachedHBaseKVStoreConfiguration matchConfig =
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
            .withCacheCapacity(15_000L)
            .withCacheExpiryTimeInSeconds(kvConfig.getCacheExpiryTimeInSeconds())
            .build();

    try {
      return NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(matchConfig, clientConfiguration);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  public static SerializableSupplier<KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>>
      createSupplier(PipelinesConfig config) {
    return () -> NameUsageMatchStoreFactory.create(config);
  }

  public static SerializableSupplier<
          Map<String, KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>>>
      createMultiServiceSupplier(PipelinesConfig config) {
    return () -> createMultipleServices(config);
  }

  public static SerializableSupplier<KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>>
      getInstanceSupplier(PipelinesConfig config) {
    return () -> NameUsageMatchStoreFactory.getInstance(config);
  }
}
