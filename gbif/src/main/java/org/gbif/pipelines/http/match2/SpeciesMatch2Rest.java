package org.gbif.pipelines.http.match2;

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
public class SpeciesMatch2Rest {

  private SpeciesMatch2Service service;

  private static SpeciesMatch2Rest INSTANCE;

  private SpeciesMatch2Rest(Config wsConfig) {

    // create client
    OkHttpClient client = HttpClientFactory.createClientWithCache(wsConfig);

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(GsonConverterFactory.create())
      .validateEagerly(true)
      .build();

    service = retrofit.create(SpeciesMatch2Service.class);
  }

  public static SpeciesMatch2Rest getInstance() {
    return getInstance(HttpConfigFactory.createConfig(Service.SPECIES_MATCH2));
  }

  public static SpeciesMatch2Rest getInstance(Config config) {
    synchronized (INSTANCE) {
      if (Objects.isNull(INSTANCE)) {
        INSTANCE = new SpeciesMatch2Rest(config);
      }
      return INSTANCE;
    }
  }

  public SpeciesMatch2Service getService() {
    return service;
  }

}
