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
import org.gbif.nameparser.NameParserGBIF;
import org.gbif.nameparser.api.NomCode;
import org.gbif.nameparser.api.ParsedName;
import org.gbif.nameparser.util.NameFormatter;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.retrofit.RetrofitClientFactory;
import org.gbif.rest.client.species.ChecklistbankService;
import org.gbif.rest.client.species.IucnRedListCategory;
import org.gbif.rest.client.species.NameUsageMatch;
import org.gbif.rest.client.species.NameUsageSearchResponse;

public class CLBSyncClient implements ChecklistbankService, Closeable {

  private final CLBMatchUsageRetrofitService clbMatchUsageRetrofitService;
  private final OkHttpClient clbOkHttpClient;
  private final Integer clbDatasetKey;
  private final Boolean outputInfragenericEpithet;
  private static final NameParserGBIF parser = new NameParserGBIF(20000, 1, 1);

  public CLBSyncClient(
      ClientConfiguration clientConfiguration,
      Integer datasetKey,
      Boolean outputInfragenericEpithet) {
    this.clbOkHttpClient = RetrofitClientFactory.createClient(clientConfiguration);
    this.clbMatchUsageRetrofitService =
        RetrofitClientFactory.createRetrofitClient(
            clbOkHttpClient,
            clientConfiguration.getBaseApiUrl(),
            CLBMatchUsageRetrofitService.class);
    this.clbDatasetKey = datasetKey;
    this.outputInfragenericEpithet = outputInfragenericEpithet;
  }

  static NameUsageMatch noMatch() {
    NameUsageMatch num = new NameUsageMatch();
    RankedName usage = new RankedName();
    usage.setKey(0);
    usage.setName("incertae sedis");
    usage.setRank(Rank.UNRANKED);
    num.setUsage(usage);
    return num;
  }

  @Override
  public NameUsageMatch match(
      Integer usageKeyIgnored, // this is ignored
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

    if (scientificName == null
        && genus == null
        && family == null
        && order == null
        && clazz == null
        && phylum == null
        && kingdom == null) {
      return noMatch();
    }

    // if scientificName is not provided, we will use the highest rank available
    if (scientificName == null) {
      if (kingdom != null) scientificName = kingdom;
      if (phylum != null) scientificName = phylum;
      if (clazz != null) scientificName = clazz;
      if (order != null) scientificName = order;
      if (family != null) scientificName = family;
      if (genus != null) scientificName = genus;
    }

    CLBUsageMatch clbUsageMatch = null;

    try {
      clbUsageMatch =
          syncCall(
              clbMatchUsageRetrofitService.match(
                  clbDatasetKey,
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
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(
          "#### Error - "
              + e.getMessage()
              + " while calling CLB service with: "
              + "kingdom="
              + kingdom
              + "&phylum="
              + phylum
              + "&class="
              + clazz
              + "&order="
              + order
              + "&family="
              + family
              + "&genus="
              + genus
              + "&scientificName="
              + scientificName
              + "&authorship="
              + scientificNameAuthorship
              + "&rank="
              + rank
              + "&verbose="
              + verbose);
      return noMatch();
    }

    if (Objects.nonNull(clbUsageMatch.getUsage()) && clbUsageMatch.match) {
      NameUsageMatch num = new NameUsageMatch();
      RankedName usage = new RankedName();
      usage.setKey(clbUsageMatch.getUsage().namesIndexId);

      if (!outputInfragenericEpithet) {
        // reconstruct the name without the infrageneric epithet
        try {
          org.gbif.nameparser.api.Rank rankParsed =
              org.gbif.nameparser.api.Rank.valueOf(
                  clbUsageMatch.getUsage().getRank().toUpperCase());
          NomCode nomCode = NomCode.valueOf(clbUsageMatch.getUsage().getCode().toUpperCase());
          ParsedName parsedName =
              parser.parse(clbUsageMatch.getUsage().getLabel(), rankParsed, nomCode);
          usage.setName(NameFormatter.canonical(parsedName));
        } catch (Exception e) {
          e.printStackTrace();
          System.err.println(
              "#### Error "
                  + e.getMessage()
                  + " + while parsing name for: "
                  + "kingdom="
                  + kingdom
                  + "&phylum="
                  + phylum
                  + "&class="
                  + clazz
                  + "&order="
                  + order
                  + "&family="
                  + family
                  + "&genus="
                  + genus
                  + "&scientificName="
                  + scientificName
                  + "&authorship="
                  + scientificNameAuthorship
                  + "&rank="
                  + rank
                  + "&verbose="
                  + verbose);
        }
      } else {
        // use the label - which includes the authorship and infrageneric epithet
        usage.setName(clbUsageMatch.getUsage().getLabel());
      }

      Rank rankParsed = Rank.valueOf(clbUsageMatch.getUsage().getRank().toUpperCase());
      usage.setRank(rankParsed);
      num.setUsage(usage);

      // set classification - handling the fact CLB now returns ranks that are not in the GBIF Rank
      // enum
      try {
        num.setClassification(
            clbUsageMatch.getUsage().getClassification().stream()
                .filter(c -> getRank(c.getRank()) != null)
                .map(
                    c -> {
                      RankedName cn = new RankedName();
                      cn.setKey(c.getNamesIndexId());
                      cn.setName(c.getName());
                      cn.setRank(Rank.valueOf(c.getRank().toUpperCase()));
                      return cn;
                    })
                .collect(Collectors.toList()));

        // add the matched taxon to the classification - this is not included in the CLB API classification
        RankedName species = new RankedName();
        species.setRank(rankParsed);
        // use the name without authorship for the species
        species.setName(clbUsageMatch.getUsage().getName());
        species.setKey(usage.getKey()); // use the usage key
        num.getClassification().add(species);

        return num;
      } catch (Exception e) {
        e.printStackTrace();

        System.err.println(
            "#### Error "
                + e.getMessage()
                + " + while setting classification for: "
                + "kingdom="
                + kingdom
                + "&phylum="
                + phylum
                + "&class="
                + clazz
                + "&order="
                + order
                + "&family="
                + family
                + "&genus="
                + genus
                + "&scientificName="
                + scientificName
                + "&authorship="
                + scientificNameAuthorship
                + "&rank="
                + rank
                + "&verbose="
                + verbose);
      }
    }
    return noMatch();
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
