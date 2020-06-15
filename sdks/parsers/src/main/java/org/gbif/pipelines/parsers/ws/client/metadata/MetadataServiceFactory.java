package org.gbif.pipelines.parsers.ws.client.metadata;

import java.util.concurrent.TimeUnit;

import org.gbif.pipelines.parsers.config.model.WsConfig;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class MetadataServiceFactory {

  private final MetadataService service;
  private static volatile MetadataServiceFactory instance;
  private static final Object MUTEX = new Object();

  private MetadataServiceFactory(WsConfig wsConfig) {

    // create client
    OkHttpClient client =
        new OkHttpClient.Builder()
            .connectTimeout(wsConfig.getTimeoutSec(), TimeUnit.SECONDS)
            .readTimeout(wsConfig.getTimeoutSec(), TimeUnit.SECONDS)
            .build();

    // create service
    Retrofit retrofit =
        new Retrofit.Builder()
            .client(client)
            .baseUrl(wsConfig.getWsUrl())
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();

    service = retrofit.create(MetadataService.class);
  }

  public static MetadataServiceFactory getInstance(WsConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new MetadataServiceFactory(config);
        }
      }
    }
    return instance;
  }

  public MetadataService getService() {
    return service;
  }
}
