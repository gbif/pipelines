package org.gbif.pipelines.core.ws.sequenceclassifier;

import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.gbif.pipelines.core.config.model.WsConfig;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class SequenceClassifierServiceFactory {
  private final SequenceClassifierService service;
  private static volatile SequenceClassifierServiceFactory instance;
  private static final Object MUTEX = new Object();

  private SequenceClassifierServiceFactory(WsConfig wsConfig) {

    // create client
    OkHttpClient client =
        new OkHttpClient.Builder()
            .connectTimeout(wsConfig.getTimeoutSec(), TimeUnit.SECONDS)
            .readTimeout(wsConfig.getTimeoutSec(), TimeUnit.SECONDS)
            .writeTimeout(wsConfig.getTimeoutSec(), TimeUnit.SECONDS)
            .build();

    // Retrofit requires the base url to end with '/'
    String baseUrl = wsConfig.getWsUrl();
    if (!baseUrl.endsWith("/")) {
      baseUrl = baseUrl + "/";
    }

    // create service
    Retrofit retrofit =
        new Retrofit.Builder()
            .client(client)
            .baseUrl(baseUrl)
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();

    service = retrofit.create(SequenceClassifierService.class);
  }

  public static SequenceClassifierServiceFactory getInstance(WsConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new SequenceClassifierServiceFactory(config);
        }
      }
    }
    return instance;
  }

  public SequenceClassifierService getService() {
    return service;
  }
}
