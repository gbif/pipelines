package org.gbif.pipelines.appender;

import java.util.concurrent.TimeUnit;

import okhttp3.OkHttpClient;
import retrofit2.Retrofit;

class HttpClientServiceRest {

  private final HttpClientService service;
  private static volatile HttpClientServiceRest instance;
  private static final Object MUTEX = new Object();

  private HttpClientServiceRest(String url, int connectTimeout, int readTimeout) {

    OkHttpClient client =
        new OkHttpClient.Builder()
            .connectTimeout(connectTimeout, TimeUnit.SECONDS)
            .readTimeout(readTimeout, TimeUnit.SECONDS)
            .build();

    Retrofit retrofit =
        new Retrofit.Builder()
            .client(client)
            .baseUrl(url)
            .validateEagerly(true)
            .build();

    service = retrofit.create(HttpClientService.class);
  }

  static HttpClientServiceRest getInstance(String url, int connectTimeout, int readTimeout) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new HttpClientServiceRest(url, connectTimeout, readTimeout);
        }
      }
    }
    return instance;
  }

  HttpClientService getService() {
    return service;
  }
}
