package org.gbif.pipelines.backbone.impact.v2;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import okhttp3.*;
import org.gbif.pipelines.backbone.impact.Identification;
import retrofit2.HttpException;
import retrofit2.Response;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class NameUsageMatchV2SyncClient implements NameUsageMatchV2Service, Closeable {

  private final NameUsageMatchV2RetrofitService nameUsageMatchV2RetrofitService;
  private final OkHttpClient clbOkHttpClient;

  public NameUsageMatchV2SyncClient(String baseUrl) {
    this.clbOkHttpClient = (new OkHttpClient.Builder())
            .connectTimeout(60000, TimeUnit.SECONDS)
            .readTimeout(60000, TimeUnit.SECONDS)
            .callTimeout(60000, TimeUnit.SECONDS).build();;

    this.nameUsageMatchV2RetrofitService =
        (new retrofit2.Retrofit.Builder())
            .client(clbOkHttpClient)
            .baseUrl(baseUrl)
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build()
            .create(NameUsageMatchV2RetrofitService.class);
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

  public static <T> T syncCall(retrofit2.Call<T> call) {
    try {
      Response<T> response = call.execute();
      if (response.isSuccessful() && response.body() != null) {
        return (T) response.body();
      } else {
        System.err.println("Service responded with an error " + response);
        throw new HttpException(response);
      }
    } catch (IOException ex) {
      throw new RuntimeException("Error executing call", ex);
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
