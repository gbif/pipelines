package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.RecordedByConfig;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.pipelines.core.functions.SerializableSupplier;

public class RecordedByKVStoreFactory {

  private final KeyValueStore<String, List<String>> kvStore;
  private static volatile RecordedByKVStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private RecordedByKVStoreFactory(ALAPipelinesConfig config) {

    long cacheSize =
        config.getRecordedByConfig() != null
            ? config.getRecordedByConfig().getCacheSizeMb()
            : RecordedByConfig.DEFAULT_CACHE_SIZE_MB;
    this.kvStore =
        KeyValueCache.cache(
            new RecordedByKVStore(),
            cacheSize,
            String.class,
            (Class<List<String>>) Collections.<String>emptyList().getClass());
  }

  public static KeyValueStore<String, List<String>> getInstance(ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new RecordedByKVStoreFactory(config);
        }
      }
    }
    return instance.kvStore;
  }

  public static SerializableSupplier<KeyValueStore<String, List<String>>> createSupplier(
      ALAPipelinesConfig config) {
    return () -> getInstance(config);
  }

  public static SerializableSupplier<KeyValueStore<String, List<String>>> getInstanceSupplier(
      ALAPipelinesConfig config) {
    return () -> RecordedByKVStoreFactory.getInstance(config);
  }
}
