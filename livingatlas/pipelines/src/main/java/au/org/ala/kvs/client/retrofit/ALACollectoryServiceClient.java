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
import java.util.Objects;
import java.util.stream.Stream;
import okhttp3.OkHttpClient;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.retrofit.RetrofitClientFactory;
import retrofit2.converter.jackson.JacksonConverterFactory;

/** Collectory service client implementation. */
public class ALACollectoryServiceClient implements ALACollectoryService {

  private final ALACollectoryRetrofitService alaCollectoryService;

  private final OkHttpClient okHttpClient;

  /**
   * Creates an instance using the provided configuration settings.
   *
   * @param clientConfiguration Rest client configuration
   */
  public ALACollectoryServiceClient(ClientConfiguration clientConfiguration) {

    okHttpClient = RetrofitClientFactory.createClient(clientConfiguration);

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
}
