package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.client.*;
import au.org.ala.kvs.client.retrofit.ALACollectoryServiceClient;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.kvs.hbase.Command;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.functions.SerializableSupplier;

/** Key value store factory for Attribution */
@Slf4j
public class ALAAttributionKVStoreFactory {

  private final KeyValueStore<String, ALACollectoryMetadata> kvStore;
  private static volatile ALAAttributionKVStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private ALAAttributionKVStoreFactory(ALAPipelinesConfig config) {
    this.kvStore = create(config);
  }

  public static KeyValueStore<String, ALACollectoryMetadata> getInstance(
      ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new ALAAttributionKVStoreFactory(config);
        }
      }
    }
    return instance.kvStore;
  }

  /** Retrieve KV Store for Collectory Metadata. */
  public static KeyValueStore<String, ALACollectoryMetadata> create(ALAPipelinesConfig config) {
    ALACollectoryServiceClient wsClient = new ALACollectoryServiceClient(config.getCollectory());
    Command closeHandler =
        () -> {
          try {
            wsClient.close();
          } catch (Exception e) {
            log.error("Unable to close", e);
          }
        };

    return cache2kBackedKVStore(wsClient, closeHandler, config);
  }

  /** Builds a KV Store backed by the rest client. */
  private static KeyValueStore<String, ALACollectoryMetadata> cache2kBackedKVStore(
      ALACollectoryService service, Command closeHandler, ALAPipelinesConfig config) {

    KeyValueStore<String, ALACollectoryMetadata> kvs =
        new KeyValueStore<String, ALACollectoryMetadata>() {
          @Override
          public ALACollectoryMetadata get(String key) {

            for (int i = 0; i < config.getCollectory().getRetryConfig().getMaxAttempts(); i++) {
              try {
                return service.lookupDataResource(key);
              } catch (retrofit2.HttpException ex) {
                log.error("HttpException looking up metadata for " + key, ex);
              } catch (Exception ex) {
                log.error("Exception looking up metadata for " + key, ex);
              }
              try {
                TimeUnit.MILLISECONDS.sleep(
                    config.getCollectory().getRetryConfig().getInitialIntervalMillis());
              } catch (Exception e) {
                //
              }
            }
            throw new PipelinesException("Unable to retrieve metadata for " + key);
          }

          @Override
          public void close() {
            closeHandler.execute();
          }
        };
    return KeyValueCache.cache(
        kvs, config.getCollectory().getCacheSizeMb(), String.class, ALACollectoryMetadata.class);
  }

  public static SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
      getInstanceSupplier(ALAPipelinesConfig config) {
    return () -> getInstance(config);
  }
}
