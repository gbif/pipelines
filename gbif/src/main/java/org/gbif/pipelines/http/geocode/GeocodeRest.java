package org.gbif.pipelines.http.geocode;

import org.gbif.pipelines.http.HttpClientFactory;
import org.gbif.pipelines.http.HttpConfigFactory;
import org.gbif.pipelines.http.config.Config;
import org.gbif.pipelines.http.config.Service;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/*
 * Singleton to create the species match 2 service.
 */
public class GeocodeRest {

  private GeocodeService service;

  private GeocodeRest() {
    // load WS Config
    Config wsConfig = HttpConfigFactory.createConfig(Service.GEO_CODE);

    // create client
    OkHttpClient client = HttpClientFactory.createClientWithCache(wsConfig);

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(GsonConverterFactory.create())
      .validateEagerly(true)
      .build();

    service = retrofit.create(GeocodeService.class);
  }

  private static class LazyHolder {
    static final GeocodeRest INSTANCE = new GeocodeRest();

  }

  public static GeocodeRest getInstance() {
    return GeocodeRest.LazyHolder.INSTANCE;
  }

  public GeocodeService getService() {
    return service;
  }

}
