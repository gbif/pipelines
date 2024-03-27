package org.gbif.pipelines.backbone.impact.clb;

import static org.gbif.rest.client.retrofit.SyncCall.syncCall;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okhttp3.OkHttpClient;
import org.gbif.api.v2.RankedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.retrofit.RetrofitClientFactory;
import org.gbif.rest.client.species.ChecklistbankService;
import org.gbif.rest.client.species.IucnRedListCategory;
import org.gbif.rest.client.species.NameUsageMatch;
import org.gbif.rest.client.species.NameUsageSearchResponse;

public class CLBSyncClient implements ChecklistbankService, Closeable {

  private final CLBMatchUsageRetrofitService clbMatchUsageRetrofitService;
  private final OkHttpClient clbOkHttpClient;

  public CLBSyncClient(ClientConfiguration clientConfiguration) {
    clbOkHttpClient = RetrofitClientFactory.createClient(clientConfiguration);
    clbMatchUsageRetrofitService =
        RetrofitClientFactory.createRetrofitClient(
            clbOkHttpClient,
            clientConfiguration.getBaseApiUrl(),
            CLBMatchUsageRetrofitService.class);
  }

  @Override
  public NameUsageMatch match(
      Integer datasetKey,
      String kingdom,
      String phylum,
      String clazz,
      String order,
      String family,
      String genus,
      String scientificName,
      String genericName,
      String specificEpithet,
      String infraspecificEpithet,
      String scientificNameAuthorship,
      String rank,
      boolean verbose,
      boolean strict) {
    CLBUsageMatch clbUsageMatch =
        syncCall(
            clbMatchUsageRetrofitService.match(
                datasetKey,
                kingdom,
                phylum,
                clazz,
                order,
                family,
                genus,
                scientificName,
                scientificNameAuthorship,
                rank,
                verbose));

    if (Objects.nonNull(clbUsageMatch.getUsage()) && clbUsageMatch.match) {
      NameUsageMatch num = new NameUsageMatch();
      RankedName usage = new RankedName();
      usage.setKey(clbUsageMatch.getUsage().namesIndexId);
      usage.setName(clbUsageMatch.getUsage().getName());
      usage.setRank(Rank.valueOf(clbUsageMatch.getUsage().getRank().toUpperCase()));
      num.setUsage(usage);

      // set classification - handling the fact CLB now returns ranks that are not in the GBIF Rank
      // enum
      num.setClassification(
          clbUsageMatch.getUsage().getClassification().stream()
              .filter(c -> getRank(c.getRank()) != null)
              .map(
                  c -> {
                    RankedName cn = new RankedName();
                    cn.setKey(c.namesIndexId);
                    cn.setName(c.getName());
                    cn.setRank(Rank.valueOf(c.getRank().toUpperCase()));
                    return cn;
                  })
              .collect(Collectors.toList()));
      return num;
    } else {
      NameUsageMatch num = new NameUsageMatch();
      RankedName usage = new RankedName();
      usage.setKey(0);
      usage.setName("Incertae sedis");
      usage.setRank(Rank.UNRANKED);
      num.setUsage(usage);
      return num;
    }
  }

  @Override
  public IucnRedListCategory getIucnRedListCategory(Integer integer) {
    return null;
  }

  @Override
  public NameUsageSearchResponse lookupNameUsage(String s, String s1) {
    return null;
  }

  public static Rank getRank(String rank) {
    for (Rank value : Rank.values()) {
      // Compare the string value of each enum constant
      if (value.name().equalsIgnoreCase(rank)) {
        return value;
      }
    }
    return null;
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
}
