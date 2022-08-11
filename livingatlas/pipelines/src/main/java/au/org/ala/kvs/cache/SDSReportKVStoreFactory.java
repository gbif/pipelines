package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.sds.api.SensitivityQuery;
import au.org.ala.sds.api.SensitivityReport;
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
public class SDSReportKVStoreFactory {

  private final KeyValueStore<SensitivityQuery, SensitivityReport> kvStore;
  private static volatile SDSReportKVStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private SDSReportKVStoreFactory(ALAPipelinesConfig config) {
    this.kvStore = create(config);
  }

  public static KeyValueStore<SensitivityQuery, SensitivityReport> getInstance(
      ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new SDSReportKVStoreFactory(config);
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
  public static KeyValueStore<SensitivityQuery, SensitivityReport> create(ALAPipelinesConfig config)
      throws IOException {
    WsConfig ws = config.getSds();
    ClientConfiguration clientConfiguration = WsUtils.createConfiguration(ws);

    ALASDSServiceClient wsClient = new ALASDSServiceClient(clientConfiguration);
    Command closeHandler =
        () -> {
          try {
            wsClient.close();
          } catch (Exception e) {
            log.warn("Unable to close", e);
          }
        };

    return cache2kBackedKVStore(wsClient, closeHandler, config);
  }

  /** Builds a KV Store backed by the rest client. */
  private static KeyValueStore<SensitivityQuery, SensitivityReport> cache2kBackedKVStore(
      ALASDSServiceClient sdsService, Command closeHandler, ALAPipelinesConfig config) {

    KeyValueStore<SensitivityQuery, SensitivityReport> kvs =
        new KeyValueStore<SensitivityQuery, SensitivityReport>() {
          @Override
          public SensitivityReport get(SensitivityQuery key) {
            try {
              return sdsService.report(key);
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
        kvs,
        config.getAlaNameMatch().getCacheSizeMb(),
        SensitivityQuery.class,
        SensitivityReport.class);
  }

  public static SerializableSupplier<KeyValueStore<SensitivityQuery, SensitivityReport>>
      getInstanceSupplier(ALAPipelinesConfig config) {
    return () -> SDSReportKVStoreFactory.getInstance(config);
  }
}
