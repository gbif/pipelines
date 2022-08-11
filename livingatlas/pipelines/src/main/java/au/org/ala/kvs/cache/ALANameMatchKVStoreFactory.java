package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.names.ws.api.NameMatchService;
import au.org.ala.names.ws.api.NameSearch;
import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.names.ws.client.ALANameUsageMatchServiceClient;
import au.org.ala.utils.WsUtils;
import au.org.ala.ws.ClientConfiguration;
import au.org.ala.ws.ClientException;
import java.io.IOException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.kvs.hbase.Command;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;

@Slf4j
public class ALANameMatchKVStoreFactory {

  private final KeyValueStore<NameSearch, NameUsageMatch> kvStore;
  private static volatile ALANameMatchKVStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private ALANameMatchKVStoreFactory(ALAPipelinesConfig config) {
    this.kvStore = create(config);
  }

  public static KeyValueStore<NameSearch, NameUsageMatch> getInstance(ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new ALANameMatchKVStoreFactory(config);
        }
      }
    }
    return instance.kvStore;
  }

  /**
   * Returns ala name matching key value store.
   *
   * @return A key value store backed by a {@link ALANameUsageMatchServiceClient}
   * @throws IOException if unable to build the client
   */
  public static KeyValueStore<NameSearch, NameUsageMatch> create(ALAPipelinesConfig config)
      throws IOException {
    WsConfig ws = config.getAlaNameMatch();
    ClientConfiguration clientConfiguration = WsUtils.createConfiguration(ws);
    ALANameUsageMatchServiceClient wsClient =
        new ALANameUsageMatchServiceClient(clientConfiguration);
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
  private static KeyValueStore<NameSearch, NameUsageMatch> cache2kBackedKVStore(
      NameMatchService nameMatchService, Command closeHandler, ALAPipelinesConfig config) {

    KeyValueStore<NameSearch, NameUsageMatch> kvs =
        new KeyValueStore<NameSearch, NameUsageMatch>() {
          @Override
          public NameUsageMatch get(NameSearch key) {
            Exception ex = null;
            try {
              for (int i = 0; i < config.getAlaNameMatch().getRetryConfig().getMaxAttempts(); i++) {
                try {
                  return nameMatchService.match(key);
                } catch (ClientException exception) {
                  log.error(
                      "ClientException contacting the species match service with key: "
                          + key.toString(),
                      exception);
                  ex = exception;
                  Thread.sleep(
                      config.getAlaNameMatch().getRetryConfig().getInitialIntervalMillis());
                } catch (Exception ex1) {
                  log.error(
                      "Exception contacting the species match service with key: " + key.toString(),
                      ex1);
                  ex = ex1;
                  Thread.sleep(5000);
                }
              }
            } catch (InterruptedException ie) {
              // NOP
            }
            throw new PipelinesException(ex);
          }

          @Override
          public void close() {
            closeHandler.execute();
          }
        };
    return KeyValueCache.cache(
        kvs, config.getAlaNameMatch().getCacheSizeMb(), NameSearch.class, NameUsageMatch.class);
  }

  public static SerializableSupplier<KeyValueStore<NameSearch, NameUsageMatch>> getInstanceSupplier(
      ALAPipelinesConfig config) {
    return () -> ALANameMatchKVStoreFactory.getInstance(config);
  }
}
