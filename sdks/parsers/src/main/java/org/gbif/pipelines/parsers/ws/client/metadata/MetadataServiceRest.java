package org.gbif.pipelines.parsers.ws.client.metadata;

import org.gbif.pipelines.parsers.ws.config.WsConfig;

import java.util.concurrent.TimeUnit;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class MetadataServiceRest {

  private final MetadataService service;
  private static volatile MetadataServiceRest instance;
  private static final Object MUTEX = new Object();

  private MetadataServiceRest(WsConfig wsConfig) {

    // create client
    OkHttpClient client =
        new OkHttpClient.Builder()
            .connectTimeout(wsConfig.getTimeout(), TimeUnit.SECONDS)
            .readTimeout(wsConfig.getTimeout(), TimeUnit.SECONDS)
            .build();

    // create service
    Retrofit retrofit =
        new Retrofit.Builder()
            .client(client)
            .baseUrl(wsConfig.getBasePath())
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();

    service = retrofit.create(MetadataService.class);
  }

  public static MetadataServiceRest getInstance(WsConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new MetadataServiceRest(config);
        }
      }
    }
    return instance;
  }

  public MetadataService getService() {
    return service;
  }
}
