package org.gbif.pipelines.kv;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.rest.client.species.NameUsageMatch;

import lombok.SneakyThrows;

/**
 * Factory to get singleton instance of KV store {@link NameUsageMatchStore}
 */
public class NameUsageMatchStoreFactory {

  private final KeyValueStore<SpeciesMatchRequest, NameUsageMatch> store;
  private static volatile NameUsageMatchStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private NameUsageMatchStoreFactory(KvConfig config) {
    store = NameUsageMatchStore.create(config);
  }

  public static NameUsageMatchStoreFactory getInstance(KvConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new NameUsageMatchStoreFactory(config);
        }
      }
    }
    return instance;
  }

  public KeyValueStore<SpeciesMatchRequest, NameUsageMatch> getStore() {
    return store;
  }

}
