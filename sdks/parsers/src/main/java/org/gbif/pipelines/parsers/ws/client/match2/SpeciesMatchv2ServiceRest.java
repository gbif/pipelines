package org.gbif.pipelines.parsers.ws.client.match2;

import org.gbif.pipelines.parsers.ws.HttpClientFactory;
import org.gbif.pipelines.parsers.ws.config.WsConfig;

import java.io.IOException;

import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/*
 * Singleton to create the species match service v2.
 */
public class SpeciesMatchv2ServiceRest {

  private static final Logger LOG = LoggerFactory.getLogger(SpeciesMatchv2ServiceRest.class);

  private final SpeciesMatchv2Service service;
  private final OkHttpClient client;
  private static volatile SpeciesMatchv2ServiceRest instance;
  private static final Object MUTEX = new Object();

  private SpeciesMatchv2ServiceRest(WsConfig wsConfig) {

    // create client
    client = HttpClientFactory.createClient(wsConfig);

    // create service
    Retrofit retrofit =
        new Retrofit.Builder()
            .client(client)
            .baseUrl(wsConfig.getBasePath())
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();

    service = retrofit.create(SpeciesMatchv2Service.class);

    // add hook to delete ws cache at shutdown
    clearCache();
  }

  public static SpeciesMatchv2ServiceRest getInstance(WsConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new SpeciesMatchv2ServiceRest(config);
        }
      }
    }
    return instance;
  }

  public SpeciesMatchv2Service getService() {
    return service;
  }

  /**
   * It deletes the ws cache.
   *
   * <p><strong>Keep in mind that this method is not thread-safe, so use it carefully.</strong>
   */
  private void clearCache() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Ws shutdown hook called");
                  if (client != null && client.cache() != null) {
                    LOG.info("Deleting ws cache");
                    try {
                      client.cache().delete();
                    } catch (IOException e) {
                      LOG.error("Ws cache could not be deleted", e);
                    }
                  }
                }));
  }
}
