package au.org.ala.kvs.client.retrofit;

import static org.gbif.rest.client.retrofit.SyncCall.syncCall;

import au.org.ala.kvs.client.*;
import au.org.ala.utils.WsUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.gbif.pipelines.core.config.model.WsConfig;

@Slf4j
/** Collectory service client implementation. */
public class ALACollectoryServiceClient implements ALACollectoryService {

  private final ALACollectoryRetrofitService alaCollectoryService;

  private final OkHttpClient okHttpClient;

  /** Creates an instance using the provided configuration settings. */
  public ALACollectoryServiceClient(WsConfig config) {
    okHttpClient = WsUtils.createOKClient(config);
    alaCollectoryService =
        WsUtils.createClient(okHttpClient, config, ALACollectoryRetrofitService.class);
  }

  /**
   * Retrieve collectory metadata
   *
   * @param dataResourceUid data resource UID
   */
  @Override
  public ALACollectoryMetadata lookupDataResource(String dataResourceUid) {
    try {
      return syncCall(alaCollectoryService.lookupDataResource(dataResourceUid));
    } catch (Exception e) {
      // this necessary due to collectory returning 500s
      // instead of 404s
      log.error("Exception thrown when calling the collectory. " + e.getMessage(), e);
      return ALACollectoryMetadata.EMPTY;
    }
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
