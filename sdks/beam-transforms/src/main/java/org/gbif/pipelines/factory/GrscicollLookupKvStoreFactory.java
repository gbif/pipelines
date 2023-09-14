package org.gbif.pipelines.factory;

import java.util.Optional;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.grscicoll.GrscicollLookupKVStoreFactory;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
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

  /* TODO Comment */
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

  /* TODO Comment */
  @SneakyThrows
  public static KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> create(
      PipelinesConfig config) {
    if (config == null) {
      return null;
    }

    String api =
        Optional.ofNullable(config.getGrscicollLookup().getApi())
            .map(WsConfig::getWsUrl)
            .orElse(config.getGbifApi().getWsUrl());

    ClientConfiguration clientConfiguration =
        ClientConfiguration.builder()
            .withBaseApiUrl(api)
            .withFileCacheMaxSizeMb(config.getGrscicollLookup().getWsCacheSizeMb())
            .withTimeOut(config.getGrscicollLookup().getWsTimeoutSec())
            .build();

    String zk = config.getGrscicollLookup().getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? config.getZkConnectionString() : zk;
    if (zk == null || config.getGrscicollLookup().isRestOnly()) {
      return GrscicollLookupKVStoreFactory.simpleGrscicollLookupKVStore(clientConfiguration);
    }

    CachedHBaseKVStoreConfiguration lookupConfig =
        CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("j") // stores JSON data
            .withHBaseKVStoreConfiguration(
                HBaseKVStoreConfiguration.builder()
                    .withTableName(config.getGrscicollLookup().getTableName())
                    .withColumnFamily("v") // Column in which qualifiers are stored
                    .withNumOfKeyBuckets(config.getGrscicollLookup().getNumOfKeyBuckets())
                    .withHBaseZk(zk)
                    .withHBaseZnode(config.getGrscicollLookup().getHbaseZnode())
                    .build())
            .withCacheCapacity(15_000L)
            .withCacheExpiryTimeInSeconds(config.getGrscicollLookup().getCacheExpiryTimeInSeconds())
            .build();

    return GrscicollLookupKVStoreFactory.simpleGrscicollLookupKVStore(
        lookupConfig, clientConfiguration);
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
