package au.org.ala.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Cache;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.gbif.pipelines.parsers.config.model.WsConfig;
import retrofit2.converter.jackson.JacksonConverterFactory;

@Slf4j
public class WsUtils {

  public static <T> T createClient(WsConfig wsConfig, Class<T> theClass) {

    OkHttpClient okHttpClient = createOKClient(wsConfig);
    // this is for https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/113
    ObjectMapper om = new ObjectMapper();
    om.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

    return (new retrofit2.Retrofit.Builder())
        .client(okHttpClient)
        .baseUrl(wsConfig.getWsUrl())
        .addConverterFactory(JacksonConverterFactory.create(om))
        .validateEagerly(true)
        .build()
        .create(theClass);
  }

  public static <T> T createClient(
      OkHttpClient okHttpClient, WsConfig wsConfig, Class<T> theClass) {

    // this is for https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/113
    ObjectMapper om = new ObjectMapper();
    om.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

    return (new retrofit2.Retrofit.Builder())
        .client(okHttpClient)
        .baseUrl(wsConfig.getWsUrl())
        .addConverterFactory(JacksonConverterFactory.create(om))
        .validateEagerly(true)
        .build()
        .create(theClass);
  }

  /**
   * Create an OKHttpClient adding headers to the requests.
   *
   * @param config
   * @return
   */
  public static OkHttpClient createOKClient(WsConfig config) {
    OkHttpClient.Builder clientBuilder =
        (new OkHttpClient.Builder())
            .connectTimeout(config.getTimeoutSec(), TimeUnit.SECONDS)
            .readTimeout(config.getTimeoutSec(), TimeUnit.SECONDS);
    clientBuilder.cache(createCache(config.getCacheSizeMb()));
    clientBuilder.addInterceptor(
        chain -> {
          Request.Builder builder = chain.request().newBuilder();
          config
              .getHttpHeaders()
              .forEach((header, headerValue) -> builder.addHeader(header, headerValue));
          Request request = builder.build();
          return chain.proceed(request);
        });
    return clientBuilder.build();
  }

  private static Cache createCache(long maxSize) {
    try {
      String cacheName = System.currentTimeMillis() + "-wsCache";
      File httpCacheDirectory = Files.createTempDirectory(cacheName).toFile();
      httpCacheDirectory.deleteOnExit();
      log.info("Cache file created - {}", httpCacheDirectory.getAbsolutePath());
      return new Cache(httpCacheDirectory, maxSize);
    } catch (IOException var4) {
      throw new IllegalStateException(
          "Cannot run without the ability to create temporary cache directory", var4);
    }
  }
}
