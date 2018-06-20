package org.gbif.pipelines.core.ws.client.match2;

import org.gbif.pipelines.core.ws.HttpClientFactory;
import org.gbif.pipelines.core.ws.config.Config;

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

  private SpeciesMatchv2ServiceRest(Config wsConfig) {

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
  }

  public static SpeciesMatchv2ServiceRest getInstance(Config config) {
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
  public static void clearCache() {
    if (instance != null && instance.client != null && instance.client.cache() != null) {
      LOG.info("Deleting species match v2 ws cache");
      try {
        instance.client.cache().delete();
      } catch (IOException e) {
        LOG.error("Species match v2 ws cache could not be deleted", e);
      }
    }
  }
}
