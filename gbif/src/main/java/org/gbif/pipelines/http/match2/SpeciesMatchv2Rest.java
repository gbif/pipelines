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
 * Singleton to create the species match service v2.
 */
public class SpeciesMatchv2Rest {

  private SpeciesMatchv2Service service;

  private static SpeciesMatchv2Rest instance;

  private SpeciesMatchv2Rest(Config wsConfig) {

    // create client
    OkHttpClient client = HttpClientFactory.createClient(wsConfig);

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(GsonConverterFactory.create())
      .validateEagerly(true)
      .build();

    service = retrofit.create(SpeciesMatchv2Service.class);
  }

  public static SpeciesMatchv2Rest getInstance() {
    return getInstance(HttpConfigFactory.createConfig(Service.SPECIES_MATCH2));
  }

  public static SpeciesMatchv2Rest getInstance(Config config) {
    synchronized (SpeciesMatchv2Rest.class) {
      if (Objects.isNull(instance)) {
        instance = new SpeciesMatchv2Rest(config);
      }
      return instance;
    }
  }

  public SpeciesMatchv2Service getService() {
    return service;
  }

}
