package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.client.ALACollectionLookup;
import au.org.ala.kvs.client.ALACollectionMatch;
import au.org.ala.kvs.client.ALACollectoryService;
import au.org.ala.kvs.client.retrofit.ALACollectoryServiceClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.kvs.hbase.Command;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.functions.SerializableSupplier;

/** Key value store factory for Collection lookups */
@Slf4j
public class ALACollectionKVStoreFactory {

  private final KeyValueStore<ALACollectionLookup, ALACollectionMatch> kvStore;
  private static volatile ALACollectionKVStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private ALACollectionKVStoreFactory(ALAPipelinesConfig config) {
    this.kvStore = create(config);
  }

  public static KeyValueStore<ALACollectionLookup, ALACollectionMatch> getInstance(
      ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new ALACollectionKVStoreFactory(config);
        }
      }
    }
    return instance.kvStore;
  }

  public static KeyValueStore<ALACollectionLookup, ALACollectionMatch> create(
      ALAPipelinesConfig config) {

    ALACollectoryServiceClient wsClient = new ALACollectoryServiceClient(config.getCollectory());
    Command closeHandler =
        () -> {
          try {
            wsClient.close();
          } catch (Exception e) {
            log.error("Unable to close", e);
            throw new PipelinesException(e);
          }
        };

    return cache2kBackedKVStore(wsClient, closeHandler, config);
  }

  /** Builds a KV Store backed by the rest client. */
  private static KeyValueStore<ALACollectionLookup, ALACollectionMatch> cache2kBackedKVStore(
      ALACollectoryService service, Command closeHandler, ALAPipelinesConfig config) {

    KeyValueStore<ALACollectionLookup, ALACollectionMatch> kvs =
        new KeyValueStore<ALACollectionLookup, ALACollectionMatch>() {
          @Override
          public ALACollectionMatch get(ALACollectionLookup key) {
            try {
              return service.lookupCodes(key.getInstitutionCode(), key.getCollectionCode());
            } catch (Exception ex) {
              // this is can happen for bad data and this service is susceptible to http 404 due to
              // the fact
              // it takes URL parameters from the raw data. So log and carry on for now.
              log.error(
                  "Error contacting the collectory service with institutionCode {} and collectionCode {} Message: {}",
                  key.getInstitutionCode(),
                  key.getCollectionCode(),
                  ex.getMessage(),
                  ex);
            }
            return ALACollectionMatch.EMPTY;
          }

          @Override
          public void close() {
            closeHandler.execute();
          }
        };
    return KeyValueCache.cache(
        kvs,
        config.getCollectory().getCacheSizeMb(),
        ALACollectionLookup.class,
        ALACollectionMatch.class);
  }

  public static SerializableSupplier<KeyValueStore<ALACollectionLookup, ALACollectionMatch>>
      getInstanceSupplier(ALAPipelinesConfig config) {
    return () -> ALACollectionKVStoreFactory.getInstance(config);
  }
}
