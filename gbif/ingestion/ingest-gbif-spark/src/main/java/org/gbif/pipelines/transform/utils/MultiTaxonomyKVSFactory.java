/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.transform.utils;

import java.io.IOException;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.LoaderRetryConfig;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.config.model.ChecklistKvConfig;
import org.gbif.pipelines.core.config.model.KvConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/** Provides the {@link KeyValueStore} as a singleton per JVM. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MultiTaxonomyKVSFactory {

  private static final Object LOCK = new Object();

  private static KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore;

  public static KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> getKvStore(
      PipelinesConfig pipelinesConfig) {
    if (kvStore == null) {
      synchronized (LOCK) {
        ChecklistKvConfig checklistKvConfig = pipelinesConfig.getNameUsageMatchingService();
        KvConfig kvConfig = checklistKvConfig.getWs();

        String api =
            Optional.ofNullable(kvConfig.getApi())
                .map(WsConfig::getWsUrl)
                .orElse(pipelinesConfig.getGbifApi().getWsUrl());

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
          log.warn("ZK config is null - wont use HBase, only REST API");
          kvStore = NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(clientConfiguration);
          return kvStore;
        }

        CachedHBaseKVStoreConfiguration.Builder matchConfigBuilder =
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
          kvStore =
              NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(
                  matchConfigBuilder.build(), clientConfiguration);
        } catch (IOException ex) {
          throw new IllegalStateException(ex);
        }
      }
    }

    return kvStore;
  }
}
