package au.org.ala.kvs.client;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.sds.api.ConservationApi;
import au.org.ala.sds.ws.client.ALASDSServiceClient;
import au.org.ala.utils.WsUtils;
import au.org.ala.ws.ClientConfiguration;
import java.io.IOException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.parsers.config.model.WsConfig;
import org.gbif.pipelines.transforms.SerializableSupplier;

@Slf4j
public class SDSConservationServiceFactory {

  private static volatile SDSConservationServiceFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private SDSConservationServiceFactory(ALAPipelinesConfig config) {}

  public static ConservationApi getInstance(ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new SDSConservationServiceFactory(config);
        }
      }
    }
    try {
      return instance.create(config);
    } catch (IOException ex) {
      throw logAndThrow(ex, "Unable to create conservation API instance");
    }
  }

  /**
   * Returns ala name matching key value store.
   *
   * @return A key value store backed by a {@link ALASDSServiceClient}
   * @throws IOException if unable to build the client
   */
  public static ConservationApi create(ALAPipelinesConfig config) throws IOException {
    WsConfig ws = config.getSds();
    ClientConfiguration clientConfiguration = WsUtils.createConfiguration(ws, config);

    ALASDSServiceClient wsClient = new ALASDSServiceClient(clientConfiguration);
    return wsClient;
  }

  public static SerializableSupplier<ConservationApi> getInstanceSupplier(
      ALAPipelinesConfig config) {
    return () -> SDSConservationServiceFactory.getInstance(config);
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
