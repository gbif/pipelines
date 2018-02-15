package org.gbif.pipelines.ws.geocode;

import org.gbif.pipelines.ws.config.Config;
import org.gbif.pipelines.ws.config.Service;
import org.gbif.pipelines.ws.config.WsConfiguration;
import org.gbif.pipelines.ws.utils.WsUtils;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/*
 * Singleton to create the species match 2 service.
 */
public enum GeocodeServiceRest {

  SINGLE;

  private GeocodeService service;

  GeocodeServiceRest() {
    init();
  }

  public GeocodeService getService() {
    return service;
  }

  void init() {
    // load WS Config
    Config wsConfig = WsConfiguration.of(Service.GEO_CODE).getConfigOrThrowException();

    // create client
    OkHttpClient client = WsUtils.createClientWithCache(wsConfig);

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(GsonConverterFactory.create())
      .validateEagerly(true)
      .build();

    service = retrofit.create(GeocodeService.class);
  }

}
