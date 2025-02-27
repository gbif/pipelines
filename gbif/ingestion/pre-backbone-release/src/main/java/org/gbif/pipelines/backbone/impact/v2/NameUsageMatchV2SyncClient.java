package org.gbif.pipelines.backbone.impact.v2;

import static org.gbif.rest.client.retrofit.SyncCall.syncCall;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import okhttp3.*;
import org.gbif.kvs.species.Identification;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.retrofit.RetrofitClientFactory;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class NameUsageMatchV2SyncClient implements NameUsageMatchV2Service, Closeable {

  private final NameUsageMatchV2RetrofitService nameUsageMatchV2RetrofitService;
  private final OkHttpClient clbOkHttpClient;

  public static OkHttpClient createClient(ClientConfiguration config) {
    OkHttpClient.Builder clientBuilder =
        (new OkHttpClient.Builder())
            .connectTimeout(config.getTimeOut(), TimeUnit.SECONDS)
            .readTimeout(config.getTimeOut(), TimeUnit.SECONDS)
            .callTimeout(config.getTimeOut(), TimeUnit.SECONDS);
    return clientBuilder.build();
  }

  public NameUsageMatchV2SyncClient(ClientConfiguration clientConfiguration) {
    this.clbOkHttpClient = createClient(clientConfiguration);


    this.nameUsageMatchV2RetrofitService =
            (new Retrofit.Builder()).client(clbOkHttpClient).baseUrl(clientConfiguration.getBaseApiUrl())
                    .addConverterFactory(JacksonConverterFactory.create()).validateEagerly(true).build().create(NameUsageMatchV2RetrofitService.class);
//
//    this.nameUsageMatchV2RetrofitService =
//        RetrofitClientFactory.createRetrofitClient(
//            this.clbOkHttpClient,
//            clientConfiguration.getBaseApiUrl(),
//            NameUsageMatchV2RetrofitService.class);
  }

  @Override
  public void close() throws IOException {
    close(clbOkHttpClient);
  }

  public void close(OkHttpClient okHttpClient) throws IOException {
    if (Objects.nonNull(okHttpClient)
        && Objects.nonNull(okHttpClient.cache())
        && Objects.nonNull(okHttpClient.cache().directory())) {
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

  @Override
  public NameUsageMatchV2 match(Identification identification) {
    try {
      return syncCall(
          nameUsageMatchV2RetrofitService.match(
              identification.getTaxonID(),
              identification.getTaxonConceptID(),
              identification.getScientificNameID(),
              identification.getScientificName(),
              identification.getScientificNameAuthorship(),
              identification.getRank(),
              identification.getGenericName(),
              identification.getSpecificEpithet(),
              identification.getInfraspecificEpithet(),
              identification.getKingdom(),
              identification.getPhylum(),
              identification.getClazz(),
              identification.getOrder(),
              identification.getFamily(),
              identification.getGenus(),
              false,
              false));

    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}
