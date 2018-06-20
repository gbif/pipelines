package org.gbif.pipelines.core.ws.client.geocode;

import org.gbif.pipelines.core.ws.HttpClientFactory;
import org.gbif.pipelines.core.ws.config.Config;

import java.io.IOException;

import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/*
 * Singleton to create the species match 2 service.
 */
public class GeocodeServiceRest {

  private static final Logger LOG = LoggerFactory.getLogger(GeocodeServiceRest.class);

  private final GeocodeService service;
  private final OkHttpClient client;
  private static volatile GeocodeServiceRest instance;
  private static final Object MUTEX = new Object();

  private GeocodeServiceRest(Config wsConfig) {

    // create client
    client = HttpClientFactory.createClient(wsConfig);

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(JacksonConverterFactory.create())
      .validateEagerly(true)
      .build();

    service = retrofit.create(GeocodeService.class);
  }

  public static GeocodeServiceRest getInstance(Config config) {
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
   * <strong>Keep in mind that this method is not thread-safe, so use it carefully.</strong>
   */
  public static void clearCache() {
    if (instance != null && instance.client != null && instance.client.cache() != null) {
      LOG.info("Deleting geocode ws cache");
      try {
        instance.client.cache().delete();
      } catch (IOException e) {
        LOG.error("Geocode ws cache could not be deleted", e);
      }
    }
  }

}
