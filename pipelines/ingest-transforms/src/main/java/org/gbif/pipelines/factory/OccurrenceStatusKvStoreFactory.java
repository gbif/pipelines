package org.gbif.pipelines.factory;

import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.parsers.config.model.PipelinesConfig;
import org.gbif.pipelines.transforms.SerializableSupplier;

import lombok.SneakyThrows;

/** Factory to get singleton instance of {@link KeyValueStore} */
public class OccurrenceStatusKvStoreFactory {

  private final KeyValueStore<String, OccurrenceStatus> keyValueStore;
  private static volatile OccurrenceStatusKvStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private OccurrenceStatusKvStoreFactory(PipelinesConfig config) {
    keyValueStore = null;
  }

  /* TODO Comment */
  public static KeyValueStore<String, OccurrenceStatus> getInstance(PipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new OccurrenceStatusKvStoreFactory(config);
        }
      }
    }
    return instance.keyValueStore;
  }

  public static SerializableSupplier<KeyValueStore<String, OccurrenceStatus>> createSupplier(
      PipelinesConfig config) {
    return () -> new OccurrenceStatusKvStoreFactory(config).keyValueStore;
  }

  public static SerializableSupplier<KeyValueStore<String, OccurrenceStatus>> getInstanceSupplier(
      PipelinesConfig config) {
    return () -> OccurrenceStatusKvStoreFactory.getInstance(config);
  }
}
