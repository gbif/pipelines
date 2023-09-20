package org.gbif.pipelines.backbone.impact;

import java.io.IOException;
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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.thrift.TException;
import org.gbif.kvs.species.Identification;
import org.gbif.rest.client.configuration.ChecklistbankClientsConfiguration;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatch;
import org.gbif.rest.client.species.retrofit.ChecklistbankServiceSyncClient;

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
                    schema,
                    options.getScope(),
                    options.getMinimumOccurrenceCount(),
                    options.getSkipKeys())));

    matched.apply(TextIO.write().to(options.getTargetDir()));

    p.run().waitUntilFinish();
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
    private final HCatSchema schema;
    private final Integer scope;
    private final int minCount;
    private final boolean skipKeys;
    private ChecklistbankServiceSyncClient service; // direct service, no cache

    MatchTransform(
        String baseAPIUrl, HCatSchema schema, Integer scope, int minCount, boolean skipKeys) {
      this.baseAPIUrl = baseAPIUrl;
      this.schema = schema;
      this.scope = scope;
      this.minCount = minCount;
      this.skipKeys = skipKeys;
    }

    @Setup
    public void setup() {
      service =
          new ChecklistbankServiceSyncClient(
              ChecklistbankClientsConfiguration.builder()
                  .nameUsageClientConfiguration(
                      ClientConfiguration.builder()
                          .withBaseApiUrl(baseAPIUrl)
                          .withFileCacheMaxSizeMb(1L)
                          .withTimeOut(120L)
                          .build())
                  .checklistbankClientConfiguration( // required but not used
                      ClientConfiguration.builder()
                          .withBaseApiUrl(baseAPIUrl)
                          .withFileCacheMaxSizeMb(1L)
                          .withTimeOut(120L)
                          .build())
                  .build());
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws HCatException {
      HCatRecord source = c.element();

      // apply the filter to only check records within scope (e.g. limit to Lepidoptera) and above
      // the limit
      long count = source.getLong("occurrenceCount", schema);
      if (count > minCount && (scope == null || taxaKeys(source, schema).contains(scope))) {

        // We use the request to ensure we apply the same "clean" operations as the production
        // pipelines, even though we short circuit the cache and use the lookup service directly.
        Identification matchRequest =
            Identification.builder()
                .withKingdom(source.getString("v_kingdom", schema))
                .withPhylum(source.getString("v_phylum", schema))
                .withClazz(source.getString("v_class", schema))
                .withOrder(source.getString("v_order", schema))
                .withFamily(source.getString("v_family", schema))
                .withGenus(source.getString("v_genus", schema))
                .withScientificName(source.getString("v_scientificName", schema))
                .withGenericName(source.getString("v_genericName", schema))
                .withSpecificEpithet(source.getString("v_specificEpithet", schema))
                .withInfraspecificEpithet(source.getString("v_infraSpecificEpithet", schema))
                .withScientificNameAuthorship(
                    source.getString("v_scientificNameAuthorship", schema))
                .withRank(source.getString("v_taxonRank", schema))
                .withVerbatimRank(source.getString("v_verbatimTaxonRank", schema))
                .withScientificNameID(source.getString("v_scientificNameID", schema))
                .withTaxonID(source.getString("v_taxonID", schema))
                .withTaxonConceptID(source.getString("v_taxonConceptID", schema))
                .build();

        try {
          // short circuit the cache, but replicate same logic of the NameUsageMatchKVStoreFactory
          NameUsageMatch usageMatch =
              service.match(
                  null, // rely only on names
                  matchRequest.getKingdom(),
                  matchRequest.getPhylum(),
                  matchRequest.getClazz(),
                  matchRequest.getOrder(),
                  matchRequest.getFamily(),
                  matchRequest.getGenus(),
                  matchRequest.getScientificName(),
                  matchRequest.getGenericName(),
                  matchRequest.getSpecificEpithet(),
                  matchRequest.getInfraspecificEpithet(),
                  matchRequest.getScientificNameAuthorship(),
                  matchRequest.getRank(),
                  false,
                  false);

          GBIFClassification existing = GBIFClassification.buildFromHive(source, schema);
          GBIFClassification proposed;
          if (usageMatch == null || isEmpty(usageMatch)) {
            proposed = GBIFClassification.newIncertaeSedis();
          } else {
            proposed = GBIFClassification.buildFromNameUsageMatch(usageMatch);
          }

          // copy pipelines to put unknown content into incertae sedis kingdom
          if (proposed.getKingdom() == null) {
            proposed.setKingdom("incertae sedis");
            proposed.setKingdomKey(0);
          }

          // emit classifications that differ, optionally considering the keys
          if (skipKeys && !existing.classificationEquals(proposed)) {
            c.output(toTabDelimited(count, matchRequest, existing, proposed, skipKeys));

          } else if (!skipKeys && !existing.equals(proposed)) {
            c.output(toTabDelimited(count, matchRequest, existing, proposed, skipKeys));
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
        long count,
        Identification verbatim,
        GBIFClassification current,
        GBIFClassification proposed,
        boolean skipKeys) {

      return String.join(
          "\t",
          String.valueOf(count),
          verbatim.getKingdom(),
          verbatim.getPhylum(),
          verbatim.getClazz(),
          verbatim.getOrder(),
          verbatim.getFamily(),
          verbatim.getGenus(),
          verbatim.getSpecificEpithet(),
          verbatim.getInfraspecificEpithet(),
          verbatim.getRank(),
          verbatim.getRank(), // avoid breaking the API (verbatimTaxonRank)
          verbatim.getScientificName(),
          verbatim.getGenericName(),
          verbatim.getScientificNameAuthorship(),
          current.toString(skipKeys),
          proposed.toString(skipKeys));
    }

    @Teardown
    public void tearDown() {
      if (Objects.nonNull(service)) {
        try {
          service.close();
        } catch (IOException ex) {
          log.error("Error closing lookup service", ex);
        }
      }
    }

    private static boolean isEmpty(NameUsageMatch response) {
      return response == null
          || response.getUsage() == null
          || (response.getClassification() == null || response.getClassification().isEmpty())
          || response.getDiagnostics() == null;
    }
  }
}
