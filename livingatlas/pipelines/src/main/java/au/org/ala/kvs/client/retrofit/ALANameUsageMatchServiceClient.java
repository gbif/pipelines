package au.org.ala.kvs.client.retrofit;

import au.org.ala.kvs.client.ALANameMatchService;
import au.org.ala.kvs.client.ALANameUsageMatch;
import au.org.ala.kvs.client.ALASpeciesMatchRequest;
import okhttp3.OkHttpClient;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.retrofit.RetrofitClientFactory;
import org.gbif.rest.client.species.NameMatchService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

import static org.gbif.rest.client.retrofit.SyncCall.syncCall;

public class ALANameUsageMatchServiceClient implements ALANameMatchService {

  //Wrapped service
  private final ALANameUsageMatchRetrofitService alaNameUsageMatchService;

  private final OkHttpClient okHttpClient;

  /**
   * Creates an instance using the provided configuration settings.
   *
   * @param clientConfiguration Rest client configuration
   */
  public ALANameUsageMatchServiceClient(ClientConfiguration clientConfiguration) {
    okHttpClient = RetrofitClientFactory.createClient(clientConfiguration);
    alaNameUsageMatchService = RetrofitClientFactory.createRetrofitClient(okHttpClient,
        clientConfiguration.getBaseApiUrl(),
        ALANameUsageMatchRetrofitService.class);
  }

  /**
   * See {@link NameMatchService#match(String, String, String, String, String, String, String,
   * String, boolean, boolean)}
   */
  @Override
  public ALANameUsageMatch match(ALASpeciesMatchRequest key) {
    return syncCall(alaNameUsageMatchService.match(key));
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
