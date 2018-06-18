package org.gbif.pipelines.core.ws.client.match2;

import org.gbif.pipelines.core.ws.HttpClientFactory;
import org.gbif.pipelines.core.ws.HttpConfigFactory;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.core.ws.config.Service;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/*
 * Singleton to create the species match service v2.
 */
public class SpeciesMatchv2ServiceRest {

  private final SpeciesMatchv2Service service;
  private static volatile SpeciesMatchv2ServiceRest instance;
  private static final Object MUTEX = new Object();

  private SpeciesMatchv2ServiceRest(Config wsConfig) {

    // create client
    OkHttpClient client = HttpClientFactory.createClient(wsConfig);

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(JacksonConverterFactory.create())
      .validateEagerly(true)
      .build();

    service = retrofit.create(SpeciesMatchv2Service.class);
  }

  public static SpeciesMatchv2ServiceRest getInstance() {
    return getInstance(HttpConfigFactory.createConfig(Service.SPECIES_MATCH2));
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

}
