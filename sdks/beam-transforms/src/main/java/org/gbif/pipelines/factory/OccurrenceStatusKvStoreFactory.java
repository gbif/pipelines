package org.gbif.pipelines.factory;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.common.parsers.OccurrenceStatusParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.transforms.SerializableSupplier;

/** Factory to get singleton instance of occurrence status {@link KeyValueStore} */
public class OccurrenceStatusKvStoreFactory {

  private final KeyValueStore<String, OccurrenceStatus> keyValueStore;
  private static volatile OccurrenceStatusKvStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private OccurrenceStatusKvStoreFactory(PipelinesConfig config) {
    // This versions will be replaced by vocabulary server in the future
    keyValueStore = OccurrenceStatusParserKvStore.create();
  }

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

  // This versions will be replaced by vocabulary server in the future
  @NoArgsConstructor(staticName = "create")
  public static class OccurrenceStatusParserKvStore
      implements KeyValueStore<String, OccurrenceStatus> {

    private final OccurrenceStatusParser parser = OccurrenceStatusParser.getInstance();

    @Override
    public OccurrenceStatus get(String s) {
      ParseResult<OccurrenceStatus> parse = parser.parse(s);
      return parse.isSuccessful() ? parse.getPayload() : null;
    }

    @Override
    public void close() {
      // NOP
    }
  }
}
