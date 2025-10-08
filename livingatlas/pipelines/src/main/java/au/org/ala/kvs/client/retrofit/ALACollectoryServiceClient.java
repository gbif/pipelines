package au.org.ala.kvs.client.retrofit;

import static au.org.ala.kvs.client.retrofit.SyncCall.syncCall;

import au.org.ala.kvs.client.*;
import au.org.ala.utils.WsUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.gbif.pipelines.core.config.model.WsConfig;

@Slf4j
/**
 * Collectory service client implementation.
 *
 * <p>Add functionality to support a minimal sandbox: - detect the absence of the collectory URL
 * config to trigger this sandbox mode. When in sandbox mode, return ALACollectoryMetadata object
 * where the name == dataResourceUid and the uniqueKey == occurrenceID.
 */
public class ALACollectoryServiceClient implements ALACollectoryService {

  private final ALACollectoryRetrofitService alaCollectoryService;

  private final OkHttpClient okHttpClient;

  /** Creates an instance using the provided configuration settings. */
  public ALACollectoryServiceClient(WsConfig config) {
    okHttpClient = WsUtils.createOKClient(config);
    if (config.getWsUrl() == null) {
      log.info("Collectory URL not configured. Running in sandbox mode.");
      alaCollectoryService = null;
    } else {
      alaCollectoryService =
          WsUtils.createClient(okHttpClient, config, ALACollectoryRetrofitService.class);
    }
  }

  /** Retrieve list of resources */
  @Override
  public List<EntityReference> listDataResources() {
    if (alaCollectoryService == null) {
      return Collections.emptyList();
    } else {
      return syncCall(alaCollectoryService.lookupDataResources());
    }
  }

  /**
   * Retrieve collectory metadata
   *
   * @param dataResourceUid data resource UID
   */
  @Override
  public ALACollectoryMetadata lookupDataResource(String dataResourceUid) {
    if (alaCollectoryService == null) {
      return ALACollectoryMetadata.builder()
          .uid(dataResourceUid)
          .contentTypes(new ArrayList<>())
          .name(dataResourceUid)
          .connectionParameters(
              ConnectionParameters.builder()
                  .termsForUniqueKey(new ArrayList<>(Collections.singleton("occurrenceID")))
                  .build())
          .build();
    } else {
      try {
        return syncCall(alaCollectoryService.lookupDataResource(dataResourceUid));
      } catch (Exception e) {
        // this necessary due to collectory returning 500s
        // instead of 404s
        log.error("Exception thrown when calling the collectory. " + e.getMessage(), e);
        return ALACollectoryMetadata.EMPTY;
      }
    }
  }

  @Override
  public ALACollectionMatch lookupCodes(String institutionCode, String collectionCode) {
    if (alaCollectoryService == null) {
      return ALACollectionMatch.EMPTY;
    } else {
      return syncCall(alaCollectoryService.lookupCodes(institutionCode, collectionCode));
    }
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
