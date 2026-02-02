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
import javax.imageio.ImageIO;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.LoaderRetryConfig;
import org.gbif.pipelines.core.config.model.KvConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.parsers.location.GeocodeKvStore;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.geocode.GeocodeResponse;

/** Provides the {@link KeyValueStore} as a singleton per JVM. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class GeocodeKVSFactory {
  private static Object LOCK = new Object();
  private static KeyValueStore<GeocodeRequest, GeocodeResponse> kvStore;

  public static KeyValueStore<GeocodeRequest, GeocodeResponse> getKvStore(PipelinesConfig config) {
    if (kvStore == null) {
      synchronized (LOCK) {
        if (kvStore == null) {

          KvConfig geocodeConfig = config.getGeocode();

          String api =
              Optional.ofNullable(geocodeConfig.getApi())
                  .map(WsConfig::getWsUrl)
                  .orElse(config.getGbifApi().getWsUrl());

          ClientConfiguration clientConfig =
              ClientConfiguration.builder()
                  .withBaseApiUrl(api)
                  .withFileCacheMaxSizeMb(geocodeConfig.getWsCacheSizeMb())
                  .withTimeOutMillisec(geocodeConfig.getWsTimeoutSec() * 1000)
                  .withMaxConnections(geocodeConfig.getMaxConnections())
                  .build();

          String zk = geocodeConfig.getZkConnectionString();
          zk = zk == null || zk.isEmpty() ? config.getZkConnectionString() : zk;
          if (zk == null || geocodeConfig.isRestOnly()) {
            log.warn("ZK config is null - wont use HBase, only REST API");
            kvStore = GeocodeKVStoreFactory.simpleGeocodeKVStore(clientConfig);
            return kvStore;
          }

          CachedHBaseKVStoreConfiguration.Builder configBuilder =
              CachedHBaseKVStoreConfiguration.builder()
                  .withValueColumnQualifier("j") // stores JSON data
                  .withHBaseKVStoreConfiguration(
                      HBaseKVStoreConfiguration.builder()
                          .withTableName(geocodeConfig.getTableName())
                          .withColumnFamily("v") // Column in which qualifiers are stored
                          .withNumOfKeyBuckets(geocodeConfig.getNumOfKeyBuckets())
                          .withHBaseZk(zk)
                          .withHBaseZnode(geocodeConfig.getHbaseZnode())
                          .build())
                  .withCacheCapacity(25_000L)
                  .withCacheExpiryTimeInSeconds(geocodeConfig.getCacheExpiryTimeInSeconds());

          KvConfig.LoaderRetryConfig retryConfig = geocodeConfig.getLoaderRetryConfig();
          if (retryConfig != null) {
            configBuilder.withLoaderRetryConfig(
                new LoaderRetryConfig(
                    retryConfig.getMaxAttempts(),
                    retryConfig.getInitialIntervalMillis(),
                    retryConfig.getMultiplier(),
                    retryConfig.getRandomizationFactor()));
          }

          try {
            KeyValueStore<GeocodeRequest, GeocodeResponse> store =
                GeocodeKVStoreFactory.simpleGeocodeKVStore(configBuilder.build(), clientConfig);

            // try and use the bitmap cache if found
            kvStore =
                GeocodeKvStore.create(
                    store,
                    ImageIO.read(
                        GeocodeKVSFactory.class.getResourceAsStream("/bitmap/bitmap.png")));

          } catch (IOException ex) {
            throw new IllegalStateException(ex);
          }
        }
      }
    }

    return kvStore;
  }
}
