package org.gbif.pipelines.core.ws.client.geocode;

import org.gbif.pipelines.core.ws.HttpClientFactory;
import org.gbif.pipelines.core.ws.HttpConfigFactory;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.core.ws.config.Service;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/*
 * Singleton to create the species match 2 service.
 */
public class GeocodeServiceRest {

  private final GeocodeService service;
  private static volatile GeocodeServiceRest instance;
  private static final Object MUTEX = new Object();

  private GeocodeServiceRest(Config wsConfig) {

    // create client
    OkHttpClient client = HttpClientFactory.createClient(wsConfig);

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(GsonConverterFactory.create())
      .validateEagerly(true)
      .build();

    service = retrofit.create(GeocodeService.class);
  }

  public static GeocodeServiceRest getInstance() {
    return getInstance(HttpConfigFactory.createConfig(Service.GEO_CODE));
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

}
