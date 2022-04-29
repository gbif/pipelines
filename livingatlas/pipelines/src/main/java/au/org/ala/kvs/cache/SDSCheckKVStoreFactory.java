package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.sds.api.SpeciesCheck;
import au.org.ala.sds.ws.client.ALASDSServiceClient;
import au.org.ala.utils.WsUtils;
import au.org.ala.ws.ClientConfiguration;
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
public class SDSCheckKVStoreFactory {

  private final KeyValueStore<SpeciesCheck, Boolean> kvStore;
  private static volatile SDSCheckKVStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private SDSCheckKVStoreFactory(ALAPipelinesConfig config) {
    this.kvStore = create(config);
  }

  public static KeyValueStore<SpeciesCheck, Boolean> getInstance(ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new SDSCheckKVStoreFactory(config);
        }
      }
    }
    return instance.kvStore;
  }

  /**
   * Returns ala name matching key value store.
   *
   * @return A key value store backed by a {@link ALASDSServiceClient}
   * @throws IOException if unable to build the client
   */
  public static KeyValueStore<SpeciesCheck, Boolean> create(ALAPipelinesConfig config)
      throws IOException {

    WsConfig ws = config.getSds();
    ClientConfiguration clientConfiguration = WsUtils.createConfiguration(ws);

    ALASDSServiceClient wsClient = new ALASDSServiceClient(clientConfiguration);
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
  private static KeyValueStore<SpeciesCheck, Boolean> cache2kBackedKVStore(
      ALASDSServiceClient sdsService, Command closeHandler, ALAPipelinesConfig config) {

    KeyValueStore<SpeciesCheck, Boolean> kvs =
        new KeyValueStore<SpeciesCheck, Boolean>() {
          @Override
          public Boolean get(SpeciesCheck key) {
            try {
              return sdsService.isSensitive(key);
            } catch (Exception ex) {
              log.error("Error contacting the sensitive data service", ex);
              throw new PipelinesException(ex);
            }
          }

          @Override
          public void close() {
            closeHandler.execute();
          }
        };
    return KeyValueCache.cache(
        kvs, config.getAlaNameMatch().getCacheSizeMb(), SpeciesCheck.class, Boolean.class);
  }

  public static SerializableSupplier<KeyValueStore<SpeciesCheck, Boolean>> getInstanceSupplier(
      ALAPipelinesConfig config) {
    return () -> SDSCheckKVStoreFactory.getInstance(config);
  }
}
