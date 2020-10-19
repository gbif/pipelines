package au.org.ala.kvs.client;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.sds.api.ConservationApi;
import au.org.ala.sds.ws.client.ALASDSServiceClient;
import au.org.ala.utils.WsUtils;
import au.org.ala.ws.ClientConfiguration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;

import java.io.IOException;

@Slf4j
public class SDSConservationServiceFactory {

  private final ConservationApi conservationApi;
  private static volatile SDSConservationServiceFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private SDSConservationServiceFactory(ALAPipelinesConfig config) {
    this.conservationApi = create(config);
  }

  public static ConservationApi getInstance(ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new SDSConservationServiceFactory(config);
        }
      }
    }
    return instance.conservationApi;
  }

  /**
   * Returns ala name matching key value store.
   *
   * @return A key value store backed by a {@link ALASDSServiceClient}
   * @throws IOException if unable to build the client
   */
  public static ConservationApi create(ALAPipelinesConfig config) throws IOException {
    WsConfig ws = config.getSds();
    ClientConfiguration clientConfiguration = WsUtils.createConfiguration(ws);

    return new ALASDSServiceClient(clientConfiguration);
  }

  public static SerializableSupplier<ConservationApi> getInstanceSupplier(
      ALAPipelinesConfig config) {
    return () -> SDSConservationServiceFactory.getInstance(config);
  }
}
