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

import java.io.IOException;
import javax.imageio.ImageIO;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.parsers.location.GeocodeKvStore;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.geocode.GeocodeResponse;

/** Provides the {@link KeyValueStore} as a singleton per JVM. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GeocodeKVSFactory {
  private static Object LOCK = new Object();
  private static KeyValueStore<GeocodeRequest, GeocodeResponse> kvStore;

  public static KeyValueStore<GeocodeRequest, GeocodeResponse> getKvStore(String api) {
    if (kvStore == null) {
      synchronized (LOCK) {
        if (kvStore == null) {
          ClientConfiguration clientConfiguration =
              ClientConfiguration.builder()
                  .withBaseApiUrl(api)
                  .withFileCacheMaxSizeMb(100L) // 100MB
                  .build();

          KeyValueStore<GeocodeRequest, GeocodeResponse> store =
              GeocodeKVStoreFactory.simpleGeocodeKVStore(clientConfiguration);
          // TODO, config and exceptions
          try {
            // try and use the bitmap cache if found
            kvStore =
                GeocodeKvStore.create(
                    store,
                    ImageIO.read(
                        GeocodeKVSFactory.class.getResourceAsStream("/bitmap/bitmap.png")));
          } catch (IOException e) {
            e.printStackTrace(); // TODO
            kvStore = store;
          }
        }
      }
    }

    return kvStore;
  }
}
