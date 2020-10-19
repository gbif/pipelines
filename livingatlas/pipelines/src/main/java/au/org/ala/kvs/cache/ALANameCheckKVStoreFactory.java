package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.names.ws.api.NameMatchService;
import au.org.ala.names.ws.api.NameSearch;
import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.names.ws.client.ALANameUsageMatchServiceClient;
import au.org.ala.ws.ClientConfiguration;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.kvs.hbase.Command;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;

/**
 * Check for names.
 *
 * <p>Each store is rank-specific. If a name is supplied, then it is checked against the nmame
 * service to see if there is something of an appropriate rank.
 */
@Slf4j
public class ALANameCheckKVStoreFactory {

  private final KeyValueStore<String, Boolean> kvStore;
  private static volatile Map<String, ALANameCheckKVStoreFactory> instances =
      new ConcurrentHashMap<>();
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private ALANameCheckKVStoreFactory(String rank, ALAPipelinesConfig config) {
    this.kvStore = create(rank, config);
  }

  public static KeyValueStore<String, Boolean> getInstance(String rank, ALAPipelinesConfig config) {
    return instances.computeIfAbsent(rank, r -> new ALANameCheckKVStoreFactory(r, config)).kvStore;
  }

  /**
   * Returns ala name matching key value store.
   *
   * @return A key value store backed by a {@link ALANameUsageMatchServiceClient}
   * @throws IOException if unasble to build the client
   */
  public static KeyValueStore<String, Boolean> create(String rank, ALAPipelinesConfig config)
      throws IOException {
    WsConfig ws = config.getAlaNameMatch();
    ClientConfiguration clientConfiguration =
        ClientConfiguration.builder()
            .baseUrl(new URL(ws.getWsUrl()))
            .timeOut(ws.getTimeoutSec() * 1000) // Geocode service connection time-out
            .cacheSize(ws.getCacheSizeMb() * 1024 * 1024)
            .build();

    ALANameUsageMatchServiceClient wsClient =
        new ALANameUsageMatchServiceClient(clientConfiguration);
    Command closeHandler =
        () -> {
          try {
            wsClient.close();
          } catch (Exception e) {
            logAndThrow(e, "Unable to close");
          }
        };

    return cache2kBackedKVStore(rank, wsClient, closeHandler, config);
  }

  /** Builds a KV Store backed by the rest client. */
  private static KeyValueStore<String, Boolean> cache2kBackedKVStore(
      final String rank,
      NameMatchService nameMatchService,
      Command closeHandler,
      ALAPipelinesConfig config) {

    KeyValueStore kvs =
        new KeyValueStore<String, Boolean>() {
          @Override
          public Boolean get(String key) {
            try {
              return nameMatchService.check(key, rank);
            } catch (Exception ex) {
              throw logAndThrow(ex, "Error contacting the species match service");
            }
          }

          @Override
          public void close() throws IOException {
            closeHandler.execute();
          }
        };
    return KeyValueCache.cache(
        kvs, config.getAlaNameMatch().getCacheSizeMb(), NameSearch.class, NameUsageMatch.class);
  }

  public static SerializableSupplier<KeyValueStore<String, Boolean>> getInstanceSupplier(
      String rank, ALAPipelinesConfig config) {
    return () -> ALANameCheckKVStoreFactory.getInstance(rank, config);
  }

  /**
   * Wraps an exception into a {@link RuntimeException}.
   *
   * @param throwable to propagate
   * @param message to log and use for the exception wrapper
   * @return a new {@link RuntimeException}
   */
  private static RuntimeException logAndThrow(Throwable throwable, String message) {
    log.error(message, throwable);
    return new RuntimeException(throwable);
  }
}
