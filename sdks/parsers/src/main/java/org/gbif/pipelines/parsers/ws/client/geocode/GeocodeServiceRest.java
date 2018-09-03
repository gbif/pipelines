package org.gbif.pipelines.parsers.ws.client.geocode;

import org.gbif.pipelines.parsers.ws.HttpClientFactory;
import org.gbif.pipelines.parsers.ws.config.WsConfig;

import java.io.IOException;

import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/*
 * Singleton to create the geo service.
 */
public class GeocodeServiceRest {

  private static final Logger LOG = LoggerFactory.getLogger(GeocodeServiceRest.class);

  private final GeocodeService service;
  private final OkHttpClient client;
  private static volatile GeocodeServiceRest instance;
  private static final Object MUTEX = new Object();

  private GeocodeServiceRest(WsConfig wsConfig) {

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

    service = retrofit.create(GeocodeService.class);

    // add hook to delete ws cache at shutdown
    clearCache();
  }

  public static GeocodeServiceRest getInstance(WsConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new GeocodeServiceRest(config);
        }
      }
    }
    return instance;
  }

  public GeocodeService getService() {
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
