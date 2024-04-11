package org.gbif.pipelines.backbone.impact.clb;

import static org.gbif.rest.client.retrofit.SyncCall.syncCall;

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.v2.RankedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.nameparser.NameParserGBIF;
import org.gbif.nameparser.api.NomCode;
import org.gbif.nameparser.api.ParsedName;
import org.gbif.nameparser.api.UnparsableNameException;
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
  private final Boolean ignoreVerbatimRank;
  private static final NameParserGBIF parser = new NameParserGBIF(20000, 1, 1);

  public CLBSyncClient(
      ClientConfiguration clientConfiguration,
      Integer datasetKey,
      Boolean outputInfragenericEpithet,
      Boolean ignoreVerbatimRank) {
    this.clbOkHttpClient = RetrofitClientFactory.createClient(clientConfiguration);
    this.clbMatchUsageRetrofitService =
        RetrofitClientFactory.createRetrofitClient(
            clbOkHttpClient,
            clientConfiguration.getBaseApiUrl(),
            CLBMatchUsageRetrofitService.class);
    this.clbDatasetKey = datasetKey;
    this.outputInfragenericEpithet = outputInfragenericEpithet;
    this.ignoreVerbatimRank = ignoreVerbatimRank;
  }

  static NameUsageMatch noMatch() {
    NameUsageMatch num = new NameUsageMatch();
    RankedName usage = new RankedName();
    usage.setKey(0);
    usage.setName("incertae sedis-clb-client");
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

    if (StringUtils.isBlank(scientificName)
        && StringUtils.isBlank(genus)
        && StringUtils.isBlank(family)
        && StringUtils.isBlank(order)
        && StringUtils.isBlank(clazz)
        && StringUtils.isBlank(phylum)
        && StringUtils.isBlank(kingdom)) {
      return noMatch();
    }

    // construct the trinomial scientific name if not provided
    if (StringUtils.isBlank(scientificName)
        && StringUtils.isNotBlank(genus)
        && StringUtils.isNotBlank(specificEpithet)
        && StringUtils.isNotBlank(infraspecificEpithet)) {
      scientificName = genus + " " + specificEpithet + " " + infraspecificEpithet;
    }

    // construct the binomial scientific name if not provided
    if (StringUtils.isBlank(scientificName)
        && StringUtils.isNotBlank(genus)
        && StringUtils.isNotBlank(specificEpithet)) {
      scientificName = genus + " " + specificEpithet;
    }

    // if scientificName is not provided, we will use the highest rank available
    if (StringUtils.isBlank(scientificName)) {
      if (StringUtils.isNotBlank(kingdom)) scientificName = kingdom;
      if (StringUtils.isNotBlank(phylum)) scientificName = phylum;
      if (StringUtils.isNotBlank(clazz)) scientificName = clazz;
      if (StringUtils.isNotBlank(order)) scientificName = order;
      if (StringUtils.isNotBlank(family)) scientificName = family;
      if (StringUtils.isNotBlank(genus)) scientificName = genus;
    }

    CLBUsageMatch clbUsageMatch = null;

    try {
      clbUsageMatch =
          matchToHigherTaxon(
              kingdom,
              phylum,
              clazz,
              order,
              family,
              genus,
              scientificName,
              scientificNameAuthorship,
              "",
              verbose);
    } catch (Exception e) {
      e.printStackTrace();
      debugUrl(
          clbDatasetKey.toString(),
          e.getMessage() + " while calling CLB service with: ",
          kingdom,
          phylum,
          clazz,
          order,
          family,
          genus,
          scientificName,
          scientificNameAuthorship,
          "");
      return noMatch();
    }

    if (!clbUsageMatch.match || Objects.isNull(clbUsageMatch.getUsage())) {
      return noMatch();
    }

    NameUsageMatch num = new NameUsageMatch();
    CLBUsage clbUsage = clbUsageMatch.getUsage();
    RankedName usage = getRankedNameFromUsage(clbUsage, outputInfragenericEpithet);
    num.setUsage(usage);

    // check if its a synonym
    if (clbUsageMatch.getUsage().getStatus() != null
        && clbUsageMatch.getUsage().getStatus().equalsIgnoreCase("synonym")) {
      // use the first taxon in the classification instead
      Optional<CLBUsage> firstInClass =
          clbUsageMatch.getUsage().classification.stream().findFirst();

      if (firstInClass.isPresent()) {
        CLBUsage acceptedCLBUsage = firstInClass.get();
        RankedName acceptedUsage =
            getRankedNameFromUsage(acceptedCLBUsage, outputInfragenericEpithet);
        num.setAcceptedUsage(acceptedUsage);
        num.setSynonym(true);
      }
    }

    // set classification - handling the fact CLB now returns ranks that are not in the GBIF Rank
    // enum
    try {
      if (clbUsageMatch.getUsage().getClassification() != null) {
        num.setClassification(
            clbUsageMatch.getUsage().getClassification().stream()
                .filter(c -> getRank(c.getRank()) != null)
                .map(
                    c -> {
                      RankedName cn = new RankedName();
                      cn.setKey(c.getNamesIndexId());

                      if (!outputInfragenericEpithet) {
                        cn.setName(getCanonical(c.getName(), c.getRank(), c.getCode()));
                      } else {
                        cn.setName(c.getName());
                      }

                      cn.setRank(Rank.valueOf(c.getRank().toUpperCase()));
                      return cn;
                    })
                .collect(Collectors.toList()));
      } else {
        num.setClassification(new ArrayList<>());
      }

      if (clbUsageMatch.getUsage().getStatus() == null
          || !clbUsageMatch.getUsage().getStatus().equalsIgnoreCase("synonym")) {
        // add the matched taxon to the classification - this is not included in the CLB API
        // classification
        RankedName leafTaxon = new RankedName();
        leafTaxon.setKey(usage.getKey()); // use the usage key
        leafTaxon.setRank(usage.getRank());
        // use the name without authorship for the species
        leafTaxon.setName(clbUsageMatch.getUsage().getName());
        if (!outputInfragenericEpithet) {
          String canonical =
              getCanonical(
                  clbUsageMatch.getUsage().getName(),
                  clbUsageMatch.getUsage().getRank(),
                  clbUsageMatch.getUsage().getCode());
          leafTaxon.setName(canonical);
        }
        num.getClassification().add(0, leafTaxon);
      }

      return num;
    } catch (Exception e) {
      e.printStackTrace();
      debugUrl(
          clbDatasetKey.toString(),
          "#### Error " + e.getMessage() + " + while setting classification with ",
          kingdom,
          phylum,
          clazz,
          order,
          family,
          genus,
          scientificName,
          scientificNameAuthorship,
          rank);
    }
    return noMatch();
  }

  private static void debugUrl(
      String datasetID,
      String msg,
      String kingdom,
      String phylum,
      String clazz,
      String order,
      String family,
      String genus,
      String scientificName,
      String scientificNameAuthorship,
      String rank) {
    System.err.println(
        msg
            + " "
            + "http://api.checklistbank.org/dataset/"
            + datasetID
            + "/match/nameusage?"
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
            + "&verbose=false");
  }

  private CLBUsageMatch matchToHigherTaxon(
      String kingdom,
      String phylum,
      String clazz,
      String order,
      String family,
      String genus,
      String scientificName,
      String scientificNameAuthorship,
      String rank,
      boolean verbose) {

    CLBUsageMatch clbUsageMatch =
        syncCall(
            clbMatchUsageRetrofitService.match(
                clbDatasetKey,
                StringUtils.isNotBlank(kingdom) ? kingdom : "",
                StringUtils.isNotBlank(phylum) ? phylum : "",
                StringUtils.isNotBlank(clazz) ? clazz : "",
                StringUtils.isNotBlank(order) ? order : "",
                StringUtils.isNotBlank(family) ? family : "",
                StringUtils.isNotBlank(genus) ? genus : "",
                scientificName,
                StringUtils.isNotBlank(scientificNameAuthorship) ? scientificNameAuthorship : "",
                ignoreVerbatimRank ? "" : rank, // the verbatim
                verbose));

    if (clbUsageMatch.match) {
      return clbUsageMatch;
    }

    List<String> linneanRanks = Lists.newArrayList(genus, family, order, clazz, phylum, kingdom);
    // try again with the next rank
    for (int i = 0; i < linneanRanks.size(); i++) {
      if (StringUtils.isNotBlank(linneanRanks.get(i))) {

        String higherTaxon = linneanRanks.get(i);
        clbUsageMatch =
            syncCall(
                clbMatchUsageRetrofitService.match(
                    clbDatasetKey,
                    i <= 5 && StringUtils.isNotBlank(kingdom) ? kingdom : "",
                    i <= 4 && StringUtils.isNotBlank(phylum) ? phylum : "",
                    i <= 3 && StringUtils.isNotBlank(clazz) ? clazz : "",
                    i <= 2 && StringUtils.isNotBlank(order) ? order : "",
                    i <= 1 && StringUtils.isNotBlank(family) ? family : "",
                    i == 0 && StringUtils.isNotBlank(genus) ? genus : "",
                    higherTaxon,
                    "",
                    "", // the verbatim
                    verbose));

        if (clbUsageMatch.match) {
          break;
        }
      }
    }

    return clbUsageMatch;
  }

  private static RankedName getRankedNameFromUsage(
      CLBUsage clbUsage, boolean outputInfragenericEpithet) {

    RankedName usage = new RankedName();
    usage.setKey(clbUsage.namesIndexId);

    if (!outputInfragenericEpithet) {
      // reconstruct the name without the infrageneric epithet
      try {
        String canonical = getCanonical(clbUsage);
        usage.setName(canonical);
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      // use the label - which includes the authorship and infrageneric epithet
      usage.setName(clbUsage.getLabel());
    }

    Rank rankParsed = Rank.UNRANKED;
    try {
      rankParsed = Rank.valueOf(clbUsage.getRank().toUpperCase());
    } catch (Exception e) {
      System.err.println("Unrecognised rank: " + clbUsage.getRank());
    }
    usage.setRank(rankParsed);
    return usage;
  }

  private static String getCanonical(CLBUsage clbUsage)
      throws UnparsableNameException, InterruptedException {
    org.gbif.nameparser.api.Rank rankParsed =
        org.gbif.nameparser.api.Rank.valueOf(clbUsage.getRank().toUpperCase());
    NomCode nomCode = null;
    if (clbUsage.getCode() != null) {
      nomCode = NomCode.valueOf(clbUsage.getCode().toUpperCase());
    }
    ParsedName parsedName = parser.parse(clbUsage.getLabel(), rankParsed, nomCode);
    String canonical = NameFormatter.canonical(parsedName);
    return canonical;
  }

  private static String getCanonical(String scientificName, String rank, String nomCodeStr) {
    try {
      org.gbif.nameparser.api.Rank rankParsed =
          org.gbif.nameparser.api.Rank.valueOf(rank.toUpperCase());
      NomCode nomCode = null;
      if (nomCodeStr != null) {
        nomCode = NomCode.valueOf(nomCodeStr.toUpperCase());
      }
      ParsedName parsedName = parser.parse(scientificName, rankParsed, nomCode);
      return NameFormatter.canonical(parsedName);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(
          "#### Error "
              + e.getMessage()
              + " + while parsing name for: "
              + scientificName
              + "&rank="
              + rank
              + "&nomCode="
              + nomCodeStr);
      return scientificName;
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
