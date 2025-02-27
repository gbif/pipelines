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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.thrift.TException;
import org.gbif.kvs.species.Identification;
import org.gbif.pipelines.backbone.impact.clb.CLBSyncClient;
import org.gbif.rest.client.configuration.ChecklistbankClientsConfiguration;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.ChecklistbankService;
import org.gbif.rest.client.species.NameUsageMatch;
import org.gbif.rest.client.species.retrofit.ChecklistbankServiceSyncClient;

/**
 * Takes the classification from verbatim data and runs it against a two species lookup services,
 * bypassing the key value caches.
 *
 * <p>Outputs a report capturing the verbatim, old service response and new service values for the
 * classifications.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NameServiceComparison {

  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(NameServiceComparisonOptions.class);
    NameServiceComparisonOptions options =
        PipelineOptionsFactory.fromArgs(args).as(NameServiceComparisonOptions.class);
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
                    options.getNewAPIBaseURI(),
                    options.getClbDatasetKey(),
                    options.getNewClbDatasetKey(),
                    options.getClbUsername(),
                    options.getClbPassword(),
                    schema,
                    options.getScope(),
                    options.getMinimumOccurrenceCount(),
                    options.getSkipKeys(),
                    options.getIgnoreWhitespace(),
                    options.getIgnoreAuthorshipFormatting(),
                    options.getOutputInfragenericEpithet(),
                    options.getIgnoreSuppliedRank())));

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
    private final String oldBaseAPIUrl;
    private final String newBaseAPIUrl;
    private final String clbDatasetKey;
    private final String newClbDatasetKey;
    private final String clbUsername;
    private final String clbPassword;
    private final HCatSchema schema;
    private final Integer scope;
    private final int minCount;
    private final boolean skipKeys;
    private final boolean ignoreWhitespace;
    private final boolean ignoreAuthorshipFormatting;
    private final boolean outputInfragenericEpithet;
    private final boolean ignoreSuppliedRank;
    private ChecklistbankService oldService; // direct service, no cache
    private ChecklistbankService newService; // direct service, no cache

    MatchTransform(
        String oldBaseAPIUrl,
        String newBaseAPIUrl,
        String clbDatasetKey,
        String newClbDatasetKey,
        String clbUsername,
        String clbPassword,
        HCatSchema schema,
        Integer scope,
        int minCount,
        boolean skipKeys,
        boolean ignoreWhitespace,
        boolean ignoreAuthorshipFormatting,
        boolean outputInfragenericEpithet,
        boolean ignoreSuppliedRank) {
      this.oldBaseAPIUrl = oldBaseAPIUrl;
      this.newBaseAPIUrl = newBaseAPIUrl;
      this.clbDatasetKey = clbDatasetKey;
      this.newClbDatasetKey = newClbDatasetKey;
      this.clbUsername = clbUsername;
      this.clbPassword = clbPassword;
      this.schema = schema;
      this.scope = scope;
      this.minCount = minCount;
      this.skipKeys = skipKeys;
      this.ignoreWhitespace = ignoreWhitespace;
      this.ignoreAuthorshipFormatting = ignoreAuthorshipFormatting;
      this.outputInfragenericEpithet = outputInfragenericEpithet;
      this.ignoreSuppliedRank = ignoreSuppliedRank;
    }

    @Setup
    public void setup() {
      // setup services to compare
      this.oldService =
          createChecklistbankService(
              oldBaseAPIUrl, clbDatasetKey, this.clbUsername, this.clbPassword);
      this.newService =
          createChecklistbankService(
              newBaseAPIUrl, newClbDatasetKey, this.clbUsername, this.clbPassword);
    }

    private ChecklistbankService createChecklistbankService(
        String baseAPIUrl, String clbDatasetKey, String clbUsername, String clbPassword) {

      if (StringUtils.isNotBlank(clbDatasetKey)) {
        ClientConfiguration clientConfiguration =
            ClientConfiguration.builder()
                .withBaseApiUrl(baseAPIUrl)
                .withTimeOut(1L)
                .withFileCacheMaxSizeMb(200L)
                .build();
        return new CLBSyncClient(
            clientConfiguration,
            clbDatasetKey,
            clbUsername,
            clbPassword,
            outputInfragenericEpithet,
            ignoreSuppliedRank);
      } else {
        return new ChecklistbankServiceSyncClient(
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
        Identification matchRequest = buildMatchRequest(source);

        try {
          // short circuit the cache, but replicate same logic of the NameUsageMatchKVStoreFactory
          NameUsageMatch oldMatch = matchWithService(oldService, matchRequest);
          NameUsageMatch newMatch = matchWithService(newService, matchRequest);

          GBIFClassification oldProposed = buildProposedClassification(oldMatch);
          GBIFClassification newProposed = buildProposedClassification(newMatch);

          // Copy pipelines to put unknown content into incertae sedis kingdom
          updateClassification(oldProposed);
          updateClassification(newProposed);

          // Emit classifications that differ
          if (shouldOutputClassification(oldProposed, newProposed)) {
            c.output(toTabDelimited(count, matchRequest, oldProposed, newProposed, skipKeys));
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

    private boolean shouldOutputClassification(
        GBIFClassification oldProposed, GBIFClassification newProposed) {
      if (skipKeys) {
        return !oldProposed.classificationEquals(
            newProposed, ignoreWhitespace, ignoreAuthorshipFormatting);
      } else {
        return !oldProposed.equals(newProposed);
      }
    }

    private Identification buildMatchRequest(HCatRecord source) throws HCatException {
      return Identification.builder()
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
          .withScientificNameAuthorship(source.getString("v_scientificNameAuthorship", schema))
          .withRank(source.getString("v_taxonRank", schema))
          .withVerbatimRank(source.getString("v_verbatimTaxonRank", schema))
          .withScientificNameID(source.getString("v_scientificNameID", schema))
          .withTaxonID(source.getString("v_taxonID", schema))
          .withTaxonConceptID(source.getString("v_taxonConceptID", schema))
          .build();
    }

    private NameUsageMatch matchWithService(
        ChecklistbankService service, Identification matchRequest) {
      return service.match(
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
    }

    private GBIFClassification buildProposedClassification(NameUsageMatch match) {
      if (match == null) {
        return GBIFClassification.error();
      } else if (isEmpty(match)) {
        return GBIFClassification.newIncertaeSedis();
      } else {
        return GBIFClassification.buildFromNameUsageMatch(match);
      }
    }

    private void updateClassification(GBIFClassification classification) {
      if (classification.getKingdom() == null) {
        classification.setKingdom("incertae sedis");
        classification.setKingdomKey("0");
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
      if (Objects.nonNull(oldService)) {
        try {
          oldService.close();
        } catch (IOException ex) {
          log.error("Error closing lookup old service", ex);
        }
      }
      if (Objects.nonNull(newService)) {
        try {
          newService.close();
        } catch (IOException ex) {
          log.error("Error closing lookup new service", ex);
        }
      }
    }

    private static boolean isEmpty(NameUsageMatch response) {
      return response.getUsage() == null
          || (response.getClassification() == null || response.getClassification().isEmpty());
      //          || response.getDiagnostics() == null;
    }
  }
}
