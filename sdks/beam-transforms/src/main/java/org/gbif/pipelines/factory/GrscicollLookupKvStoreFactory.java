package org.gbif.pipelines.factory;

import java.util.Optional;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration.Builder;
import org.gbif.kvs.grscicoll.GrscicollLookupKVStoreFactory;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.LoaderRetryConfig;
import org.gbif.pipelines.core.config.model.KvConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;

/**
 * Factory to get singleton instance of KV store {@link KeyValueStore} for the GrSciColl Lookup
 * Service.
 */
public class GrscicollLookupKvStoreFactory {

  private final KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> kvStore;
  private static volatile GrscicollLookupKvStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private GrscicollLookupKvStoreFactory(PipelinesConfig config) {
    this.kvStore = create(config);
  }

  public static KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> getInstance(
      PipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new GrscicollLookupKvStoreFactory(config);
        }
      }
    }
    return instance.kvStore;
  }

  @SneakyThrows
  public static KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> create(
      PipelinesConfig config) {
    if (config == null) {
      return null;
    }

    KvConfig grscicollLookupConfig = config.getGrscicollLookup();

    String api =
        Optional.ofNullable(grscicollLookupConfig.getApi())
            .map(WsConfig::getWsUrl)
            .orElse(config.getGbifApi().getWsUrl());

    ClientConfiguration clientConfiguration =
        ClientConfiguration.builder()
            .withBaseApiUrl(api)
            .withFileCacheMaxSizeMb(grscicollLookupConfig.getWsCacheSizeMb())
            .withTimeOutMillisec(grscicollLookupConfig.getWsTimeoutSec() * 1000)
            .build();

    String zk = grscicollLookupConfig.getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? config.getZkConnectionString() : zk;
    if (zk == null || grscicollLookupConfig.isRestOnly()) {
      return GrscicollLookupKVStoreFactory.simpleGrscicollLookupKVStore(clientConfiguration);
    }

    Builder configBuilder =
        CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("j") // stores JSON data
            .withHBaseKVStoreConfiguration(
                HBaseKVStoreConfiguration.builder()
                    .withTableName(grscicollLookupConfig.getTableName())
                    .withColumnFamily("v") // Column in which qualifiers are stored
                    .withNumOfKeyBuckets(grscicollLookupConfig.getNumOfKeyBuckets())
                    .withHBaseZk(zk)
                    .withHBaseZnode(grscicollLookupConfig.getHbaseZnode())
                    .build())
            .withCacheCapacity(25_000L)
            .withCacheExpiryTimeInSeconds(grscicollLookupConfig.getCacheExpiryTimeInSeconds());

    KvConfig.LoaderRetryConfig retryConfig = grscicollLookupConfig.getLoaderRetryConfig();
    if (retryConfig != null) {
      configBuilder.withLoaderRetryConfig(
          new LoaderRetryConfig(
              retryConfig.getMaxAttempts(),
              retryConfig.getInitialIntervalMillis(),
              retryConfig.getMultiplier(),
              retryConfig.getRandomizationFactor()));
    }

    return GrscicollLookupKVStoreFactory.simpleGrscicollLookupKVStore(
        configBuilder.build(), clientConfiguration);
  }

  public static SerializableSupplier<KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse>>
      createSupplier(PipelinesConfig config) {
    return () -> GrscicollLookupKvStoreFactory.create(config);
  }

  public static SerializableSupplier<KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse>>
      getInstanceSupplier(PipelinesConfig config) {
    return () -> GrscicollLookupKvStoreFactory.getInstance(config);
  }
}
