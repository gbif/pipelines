package au.org.ala.kvs.client.retrofit;

import static org.gbif.rest.client.retrofit.SyncCall.syncCall;

import au.org.ala.kvs.client.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.gbif.rest.client.configuration.ClientConfiguration;
import retrofit2.converter.jackson.JacksonConverterFactory;

@Slf4j
/** Collectory service client implementation. */
public class ALACollectoryServiceClient implements ALACollectoryService {

  private final ALACollectoryRetrofitService alaCollectoryService;

  private final OkHttpClient okHttpClient;

  /**
   * Creates an instance using the provided configuration settings.
   *
   * @param clientConfiguration Rest client configuration
   */
  public ALACollectoryServiceClient(
      ClientConfiguration clientConfiguration, Map<String, String> headers) {

    okHttpClient = createClient(clientConfiguration, headers);

    // this is for https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/113
    ObjectMapper om = new ObjectMapper();
    om.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

    alaCollectoryService =
        (new retrofit2.Retrofit.Builder())
            .client(okHttpClient)
            .baseUrl(clientConfiguration.getBaseApiUrl())
            .addConverterFactory(JacksonConverterFactory.create(om))
            .validateEagerly(true)
            .build()
            .create(ALACollectoryRetrofitService.class);
  }

  /**
   * Retrieve collectory metadata
   *
   * @param dataResourceUid data resource UID
   */
  @Override
  public ALACollectoryMetadata lookupDataResource(String dataResourceUid) {
    return syncCall(alaCollectoryService.lookupDataResource(dataResourceUid));
  }

  @Override
  public ALACollectionMatch lookupCodes(String institutionCode, String collectionCode) {
    return syncCall(alaCollectoryService.lookupCodes(institutionCode, collectionCode));
  }

  @Override
  public void close() throws IOException {
    if (Objects.nonNull(okHttpClient) && Objects.nonNull(okHttpClient.cache())) {
      File cacheDirectory = okHttpClient.cache().directory();
      if (cacheDirectory.exists()) {
        try (Stream<File> files =
            Files.walk(cacheDirectory.toPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)) {
          files.forEach(File::delete);
        }
      }
    }
  }

  public static OkHttpClient createClient(ClientConfiguration config, Map<String, String> headers) {
    OkHttpClient.Builder clientBuilder =
        (new OkHttpClient.Builder())
            .connectTimeout(config.getTimeOut(), TimeUnit.SECONDS)
            .readTimeout(config.getTimeOut(), TimeUnit.SECONDS);
    clientBuilder.cache(createCache(config.getFileCacheMaxSizeMb()));
    clientBuilder.addInterceptor(
        chain -> {
          Request.Builder builder = chain.request().newBuilder();
          headers.forEach((header, headerValue) -> builder.addHeader(header, headerValue));
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
