package org.gbif.pipelines.transform.utils;

import java.io.IOException;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.grscicoll.GrscicollLookupKVStoreFactory;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.LoaderRetryConfig;
import org.gbif.pipelines.core.config.model.KvConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GrscicollLookupKvStoreFactory {

  private static KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> kvStore;
  private static final Object LOCK = new Object();

  public static KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> getKvStore(
      PipelinesConfig config) {
    if (kvStore == null) {
      synchronized (LOCK) {
        if (kvStore == null) {

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
            kvStore =
                GrscicollLookupKVStoreFactory.simpleGrscicollLookupKVStore(clientConfiguration);
            return kvStore;
          }

          CachedHBaseKVStoreConfiguration.Builder configBuilder =
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
                  .withCacheExpiryTimeInSeconds(
                      grscicollLookupConfig.getCacheExpiryTimeInSeconds());

          KvConfig.LoaderRetryConfig retryConfig = grscicollLookupConfig.getLoaderRetryConfig();
          if (retryConfig != null) {
            configBuilder.withLoaderRetryConfig(
                new LoaderRetryConfig(
                    retryConfig.getMaxAttempts(),
                    retryConfig.getInitialIntervalMillis(),
                    retryConfig.getMultiplier(),
                    retryConfig.getRandomizationFactor()));
          }

          try {
            kvStore =
                GrscicollLookupKVStoreFactory.simpleGrscicollLookupKVStore(
                    configBuilder.build(), clientConfiguration);
          } catch (IOException ex) {
            throw new IllegalStateException(ex);
          }
        }
      }
    }
    return kvStore;
  }
}
