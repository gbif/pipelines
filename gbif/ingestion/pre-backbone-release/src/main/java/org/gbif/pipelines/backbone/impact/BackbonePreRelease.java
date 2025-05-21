package org.gbif.pipelines.backbone.impact;

import feign.FeignException;
import java.io.*;
import java.net.URLEncoder;
import java.util.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.thrift.TException;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.RestClientFactory;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchingService;

/**
 * Takes the classification from verbatim data and runs it against a species lookup service
 * bypassing the key value cache. Outputs a report capturing the verbatim, current and "new"" values
 * for the classifications.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BackbonePreRelease {

  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(BackbonePreReleaseOptions.class);
    BackbonePreReleaseOptions options =
        PipelineOptionsFactory.fromArgs(args).as(BackbonePreReleaseOptions.class);
    options.setRunner(SparkRunner.class);
    String hdfsPath = options.getHdfsSiteConfig();
    String corePath = options.getCoreSiteConfig();
    Configuration conf = new Configuration(false);
    conf.addResource(new Path(hdfsPath));
    conf.addResource(new Path(corePath));
    options.setHdfsConfiguration(Collections.singletonList(conf));

    // delete previous runs
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(new Path(options.getTargetDir()))) {
      log.info("Deleting previous run: " + options.getTargetDir());
      hdfs.delete(new Path(options.getTargetDir()), true);
    }

    Pipeline p = Pipeline.create(options);

    final HCatSchema schema = readSchema(options);

    PCollection<HCatRecord> records =
        p.apply(
            HCatalogIO.read()
                .withConfigProperties(
                    Collections.singletonMap(
                        HiveConf.ConfVars.METASTOREURIS.varname, options.getMetastoreUris()))
                .withDatabase(options.getDatabase())
                .withTable(options.getTable()));

    PCollection<String> matched =
        records.apply(
            "Lookup names",
            ParDo.of(
                new MatchTransform(
                    options.getAPIBaseURI(),
                    options.getChecklistKey(),
                    schema,
                    options.getScope(),
                    options.getMinimumOccurrenceCount(),
                    options.getSkipKeys(),
                    options.getIgnoreWhitespace(),
                    options.getIgnoreAuthorshipFormatting())));

    matched.apply(TextIO.write().to(options.getTargetDir() + "/impact").withSuffix(".csv"));

    p.run().waitUntilFinish();

    // Merge all the files into a single CSV file and append header
    buildCSVFile(options, FileSystem.get(conf));
  }

  /** Formats the data for the output line in the CSV. */
  private static String toHeader(boolean skipKeys) {
    return String.join(
        "\t",
        "count",
        "verbatim_taxonID",
        "verbatim_taxonConceptID",
        "verbatim_scientificNameID",
        "verbatim_kingdom",
        "verbatim_phylum",
        "verbatim_class",
        "verbatim_order",
        "verbatim_family",
        "verbatim_genus",
        "verbatim_specificEpithet",
        "verbatim_infraspecificEpithet",
        "verbatim_rank",
        "verbatim_verbatimRank",
        "verbatim_scientificName",
        "verbatim_genericName",
        "verbatim_author",
        GBIFClassification.toHeader("current_", skipKeys),
        GBIFClassification.toHeader("proposed_", skipKeys),
        "debug_url");
  }

  private static void buildCSVFile(BackbonePreReleaseOptions options, FileSystem hdfs)
      throws IOException {

    Path hdfsDir = new Path(options.getTargetDir());
    FileStatus[] files = hdfs.listStatus(hdfsDir);
    FSDataOutputStream out =
        hdfs.create(new Path(options.getTargetDir() + "/" + options.getReportFileName()), true);

    // write the header
    out.write((toHeader(options.getSkipKeys()) + "\n").getBytes());

    for (FileStatus file : files) {
      if (!file.isFile()) continue; // Skip subdirectories
      try (FSDataInputStream in = hdfs.open(file.getPath())) {
        log.info("Merging file: " + file.getPath());
        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) > 0) {
          out.write(buffer, 0, bytesRead);
        }
      }
    }

    out.close();
    log.info("Merged files into: " + options.getTargetDir() + "/" + options.getReportFileName());
  }

  private static HCatSchema readSchema(BackbonePreReleaseOptions options)
      throws HCatException, TException {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, options.getMetastoreUris());
    HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConf);
    List<FieldSchema> fieldSchemaList =
        metaStoreClient.getSchema(options.getDatabase(), options.getTable());
    return HCatSchemaUtils.getHCatSchema(fieldSchemaList);
  }

  /** Performs the lookup. */
  static class MatchTransform extends DoFn<HCatRecord, String> {
    private final String baseAPIUrl;
    private final String checklistKey;
    private final HCatSchema schema;
    private final Integer scope;
    private final int minCount;
    private final boolean skipKeys;
    private final boolean ignoreWhitespace;
    private final boolean ignoreAuthorshipFormatting;
    private NameUsageMatchingService service; // direct service, no cache

    MatchTransform(
        String baseAPIUrl,
        String checklistKey,
        HCatSchema schema,
        Integer scope,
        int minCount,
        boolean skipKeys,
        boolean ignoreWhitespace,
        boolean ignoreAuthorshipFormatting) {
      this.baseAPIUrl = baseAPIUrl;
      this.checklistKey = checklistKey;
      this.schema = schema;
      this.scope = scope;
      this.minCount = minCount;
      this.skipKeys = skipKeys;
      this.ignoreWhitespace = ignoreWhitespace;
      this.ignoreAuthorshipFormatting = ignoreAuthorshipFormatting;
    }

    @Setup
    public void setup() {
      service =
          RestClientFactory.createNameMatchService(
              ClientConfiguration.builder()
                  .withBaseApiUrl(baseAPIUrl)
                  .withFileCacheMaxSizeMb(1L)
                  .withTimeOutMillisec(120_1000)
                  .build());
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws HCatException {
      HCatRecord source = c.element();

      // apply the filter to only check records within scope (e.g. limit to Lepidoptera) and above
      // the limit
      long count = source.getLong("occurrencecount", schema);
      if (count > minCount && (scope == null || taxaKeys(source, schema).contains(scope))) {

        // We use the request to ensure we apply the same "clean" operations as the production
        // pipelines, even though we short circuit the cache and use the lookup service directly.
        NameUsageMatchRequest matchRequest =
            NameUsageMatchRequest.builder()
                .withKingdom(source.getString("v_kingdom", schema))
                .withPhylum(source.getString("v_phylum", schema))
                .withClazz(source.getString("v_class", schema))
                .withOrder(source.getString("v_order", schema))
                .withFamily(source.getString("v_family", schema))
                .withGenus(source.getString("v_genus", schema))
                .withScientificName(source.getString("v_scientificname", schema))
                .withGenericName(source.getString("v_genericname", schema))
                .withSpecificEpithet(source.getString("v_specificepithet", schema))
                .withInfraspecificEpithet(source.getString("v_infraspecificepithet", schema))
                .withRank(source.getString("v_taxonrank", schema))
                .withVerbatimRank(source.getString("v_verbatimtaxonrank", schema))
                .withScientificNameID(source.getString("v_scientificnameid", schema))
                .withTaxonID(source.getString("v_taxonid", schema))
                .withTaxonConceptID(source.getString("v_taxonconceptid", schema))
                .withChecklistKey(checklistKey)
                .build();

        try {
          // short circuit the cache, but replicate same logic of the NameUsageMatchKVStoreFactory
          NameUsageMatchResponse usageMatch = null;
          try {
            usageMatch = service.match(matchRequest);
          } catch (FeignException.InternalServerError e) {
            // Handle 500 Internal Server Error
            System.out.println("Caught 500 Internal Server Error: " + e.getMessage());
          }

          GBIFClassification existing = GBIFClassification.buildFromHiveSource(source, schema);
          GBIFClassification proposed;
          if (usageMatch == null) {
            proposed = GBIFClassification.error();
          } else if (isEmpty(usageMatch)) {
            proposed = GBIFClassification.newIncertaeSedis();
          } else {
            proposed = GBIFClassification.buildFromNameUsageMatch(usageMatch);
          }

          // copy pipelines to put unknown content into incertae sedis kingdom
          if (proposed.getKingdom() == null) {
            System.out.println("##### Failed request: " + toDebugUrl(baseAPIUrl, matchRequest));
            proposed.setKingdom("incertae sedis");
            proposed.setKingdomKey("0");
          }

          // emit classifications that differ, optionally considering the keys
          if (skipKeys
              && !existing.classificationEquals(
                  proposed, ignoreWhitespace, ignoreAuthorshipFormatting)) {
            c.output(
                toTabDelimited(
                    baseAPIUrl, count, matchRequest, existing, proposed, skipKeys, matchRequest));
          } else if (!skipKeys && !existing.equals(proposed)) {
            c.output(
                toTabDelimited(
                    baseAPIUrl, count, matchRequest, existing, proposed, skipKeys, matchRequest));
          }

        } catch (Exception e) {
          // console logging to simplify spark diagnostics
          System.err.println("Lookup failed:");
          System.err.println(matchRequest);
          e.printStackTrace();
          throw e; // fail the job
        }
      }
    }

    public static String toDebugUrl(String apiUrl, NameUsageMatchRequest matchRequest) {
      StringBuilder url = new StringBuilder(apiUrl + "/v2/species/match?");

      try {
        if (matchRequest.getTaxonID() != null) {
          url.append("taxonID=")
              .append(URLEncoder.encode(matchRequest.getTaxonID(), "UTF-8"))
              .append("&");
        }
        if (matchRequest.getTaxonConceptID() != null) {
          url.append("taxonConceptID=")
              .append(URLEncoder.encode(matchRequest.getTaxonConceptID(), "UTF-8"))
              .append("&");
        }
        if (matchRequest.getScientificNameID() != null) {
          url.append("scientificNameID=")
              .append(URLEncoder.encode(matchRequest.getScientificNameID(), "UTF-8"))
              .append("&");
        }
        if (matchRequest.getKingdom() != null) {
          url.append("kingdom=")
              .append(URLEncoder.encode(matchRequest.getKingdom(), "UTF-8"))
              .append("&");
        }
        if (matchRequest.getPhylum() != null) {
          url.append("phylum=")
              .append(URLEncoder.encode(matchRequest.getPhylum(), "UTF-8"))
              .append("&");
        }
        if (matchRequest.getClazz() != null) {
          url.append("class=")
              .append(URLEncoder.encode(matchRequest.getClazz(), "UTF-8"))
              .append("&");
        }
        if (matchRequest.getOrder() != null) {
          url.append("order=")
              .append(URLEncoder.encode(matchRequest.getOrder(), "UTF-8"))
              .append("&");
        }
        if (matchRequest.getFamily() != null) {
          url.append("family=")
              .append(URLEncoder.encode(matchRequest.getFamily(), "UTF-8"))
              .append("&");
        }
        if (matchRequest.getGenus() != null) {
          url.append("genus=")
              .append(URLEncoder.encode(matchRequest.getGenus(), "UTF-8"))
              .append("&");
        }
        if (matchRequest.getScientificName() != null) {
          url.append("scientificName=")
              .append(URLEncoder.encode(matchRequest.getScientificName(), "UTF-8"))
              .append("&");
        }
        if (matchRequest.getRank() != null) {
          url.append("rank=")
              .append(URLEncoder.encode(matchRequest.getRank(), "UTF-8"))
              .append("&");
        }
        url.append("checklistKey=")
            .append(URLEncoder.encode(matchRequest.getChecklistKey(), "UTF-8"))
            .append("&");
        url.append("verbose=true");

      } catch (UnsupportedEncodingException e) {
        // UTF-8 is guaranteed to be supported, so this shouldn't happen.
        throw new RuntimeException("UTF-8 encoding not supported", e);
      }

      return url.toString();
    }

    /** Extracts all taxon keys from the record. */
    private static Set<Integer> taxaKeys(HCatRecord record, HCatSchema schema)
        throws HCatException {
      HashSet<Integer> keys = new HashSet<>();
      if (record.getInteger("kingdomKey", schema) != null)
        keys.add(record.getInteger("kingdomKey", schema));
      if (record.getInteger("phylumKey", schema) != null)
        keys.add(record.getInteger("phylumKey", schema));
      if (record.getInteger("classKey", schema) != null)
        keys.add(record.getInteger("classKey", schema));
      if (record.getInteger("orderKey", schema) != null)
        keys.add(record.getInteger("orderKey", schema));
      if (record.getInteger("familyKey", schema) != null)
        keys.add(record.getInteger("familyKey", schema));
      if (record.getInteger("genusKey", schema) != null)
        keys.add(record.getInteger("genusKey", schema));
      if (record.getInteger("subGenusKey", schema) != null)
        keys.add(record.getInteger("subGenusKey", schema));
      if (record.getInteger("speciesKey", schema) != null)
        keys.add(record.getInteger("speciesKey", schema));
      if (record.getInteger("taxonKey", schema) != null)
        keys.add(record.getInteger("taxonKey", schema));
      return keys;
    }

    /** Formats the data for the output line in the CSV. */
    private static String toTabDelimited(
        String baseAPIUrl,
        long count,
        NameUsageMatchRequest verbatim,
        GBIFClassification current,
        GBIFClassification proposed,
        boolean skipKeys,
        NameUsageMatchRequest matchRequest) {

      return String.join(
          "\t",
          String.valueOf(count),
          safe(verbatim.getTaxonID()),
          safe(verbatim.getTaxonConceptID()),
          safe(verbatim.getScientificNameID()),
          safe(verbatim.getKingdom()),
          safe(verbatim.getPhylum()),
          safe(verbatim.getClazz()),
          safe(verbatim.getOrder()),
          safe(verbatim.getFamily()),
          safe(verbatim.getGenus()),
          safe(verbatim.getSpecificEpithet()),
          safe(verbatim.getInfraspecificEpithet()),
          safe(verbatim.getRank()),
          safe(verbatim.getRank()), // avoid breaking the API (verbatimTaxonRank)
          safe(verbatim.getScientificName()),
          safe(verbatim.getGenericName()),
          safe(verbatim.getAuthorship()),
          current.toString(skipKeys),
          proposed.toString(skipKeys),
          safe(toDebugUrl(baseAPIUrl, matchRequest)));
    }

    /** Returns the string or an empty string if null. */
    private static String safe(String value) {
      //      return value != null && !value.equalsIgnoreCase("null") ? value : "";
      return value;
    }

    private static boolean isEmpty(NameUsageMatchResponse response) {
      return response.getUsage() == null
          || (response.getClassification() == null || response.getClassification().isEmpty());
    }
  }
}
