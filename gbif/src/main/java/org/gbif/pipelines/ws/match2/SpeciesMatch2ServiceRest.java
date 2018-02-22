package org.gbif.pipelines.ws.match2;

import org.gbif.pipelines.ws.config.Config;
import org.gbif.pipelines.ws.config.Service;
import org.gbif.pipelines.ws.config.WsConfigurer;
import org.gbif.pipelines.ws.utils.WsUtils;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/*
 * Singleton to create the species match 2 service.
 */
public enum SpeciesMatch2ServiceRest {

  SINGLE;

  private SpeciesMatch2Service service;

  SpeciesMatch2ServiceRest() {
    init();
  }

  public SpeciesMatch2Service getService() {
    return service;
  }

  void init() {
    // load WS Config
    Config wsConfig = WsConfigurer.createConfig(Service.SPECIES_MATCH2);

    // create client
    OkHttpClient client = WsUtils.createClientWithCache(wsConfig);

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(GsonConverterFactory.create())
      .validateEagerly(true)
      .build();

    service = retrofit.create(SpeciesMatch2Service.class);

  }

}
