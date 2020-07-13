package au.org.ala.kvs.client.retrofit;

import au.org.ala.kvs.client.*;
import okhttp3.OkHttpClient;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.retrofit.RetrofitClientFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

import static org.gbif.rest.client.retrofit.SyncCall.syncCall;

/**
 * Collectory service client implementation.
 */
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
    alaCollectoryService = RetrofitClientFactory.createRetrofitClient(okHttpClient,
        clientConfiguration.getBaseApiUrl(),
        ALACollectoryRetrofitService.class);
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
        try (Stream<File> files = Files.walk(cacheDirectory.toPath())
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)) {
          files.forEach(File::delete);
        }
      }
    }
  }
}
