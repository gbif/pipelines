package org.gbif.pipelines.http.geocode;

import org.gbif.pipelines.http.HttpClientFactory;
import org.gbif.pipelines.http.HttpConfigFactory;
import org.gbif.pipelines.http.config.Config;
import org.gbif.pipelines.http.config.Service;

import java.util.Objects;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/*
 * Singleton to create the species match 2 service.
 */
public class GeocodeRest {

  private GeocodeService service;

  private static GeocodeRest instance;

  private GeocodeRest(Config wsConfig) {

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

  public static GeocodeRest getInstance() {
    return getInstance(HttpConfigFactory.createConfig(Service.GEO_CODE));
  }

  public static GeocodeRest getInstance(Config config) {
    synchronized (GeocodeRest.class) {
      if (Objects.isNull(instance)) {
        instance = new GeocodeRest(config);
      }
      return instance;
    }
  }

  public GeocodeService getService() {
    return service;
  }

}
