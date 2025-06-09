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
package org.gbif.pipelines.interpretation.transform.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/** Provides the {@link KeyValueStore} as a singleton per JVM. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MultiTaxonomyKVSFactory {

  private static Object LOCK = new Object();

  private static KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore;

  public static KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> getKvStore(
      String api) {
    if (kvStore == null) {
      synchronized (LOCK) {
        if (kvStore == null) {
          ClientConfiguration clientConfiguration =
              ClientConfiguration.builder()
                  .withBaseApiUrl(api)
                  .withFileCacheMaxSizeMb(100L) // 100MB
                  .build();
          kvStore = NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(clientConfiguration);
        }
      }
    }

    return kvStore;
  }
}
