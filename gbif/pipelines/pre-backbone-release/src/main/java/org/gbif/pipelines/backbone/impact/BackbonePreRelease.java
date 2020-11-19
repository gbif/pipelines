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
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatch;

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
                    options.getMinimumOccurrenceCount())));

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
    private KeyValueStore<SpeciesMatchRequest, NameUsageMatch> service;

    MatchTransform(String baseAPIUrl, HCatSchema schema, Integer scope, int minCount) {
      this.baseAPIUrl = baseAPIUrl;
      this.schema = schema;
      this.scope = scope;
      this.minCount = minCount;
    }

    @Setup
    public void setup() {
      service =
          NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(
              ClientConfiguration.builder()
                  .withBaseApiUrl(baseAPIUrl)
                  .withFileCacheMaxSizeMb(1L)
                  .withTimeOut(120L)
                  .build());
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws HCatException {
      HCatRecord source = c.element();

      // apply the filter to only check records within scope (e.g. limit to Lepidoptera) and above
      // the limit
      long count = source.getLong("occurrenceCount", schema);
      if (count > minCount && (scope == null || taxaKeys(source, schema).contains(scope))) {

        SpeciesMatchRequest matchRequest =
            SpeciesMatchRequest.builder()
                .withKingdom(source.getString("v_kingdom", schema))
                .withPhylum(source.getString("v_phylum", schema))
                .withClazz(source.getString("v_class", schema))
                .withOrder(source.getString("v_order", schema))
                .withFamily(source.getString("v_family", schema))
                .withGenus(source.getString("v_genus", schema))
                .withScientificName(source.getString("v_scientificName", schema))
                .withRank(source.getString("v_taxonRank", schema))
                .withVerbatimRank(source.getString("v_verbatimTaxonRank", schema))
                .withSpecificEpithet(source.getString("v_specificEpithet", schema))
                .withInfraspecificEpithet(source.getString("v_infraSpecificEpithet", schema))
                .withScientificNameAuthorship(
                    source.getString("v_scientificNameAuthorship", schema))
                .build();

        NameUsageMatch usageMatch = service.get(matchRequest);

        GBIFClassification existing = GBIFClassification.buildFromHive(source, schema);
        GBIFClassification proposed;
        if (usageMatch == null || isEmpty(usageMatch)) {
          proposed = GBIFClassification.newIncertaeSedis();
        } else {
          proposed = GBIFClassification.buildFromNameUsageMatch(usageMatch);
        }

        // if classifications differ, then we log them in the report otherwise we skip them.
        if (!existing.equals(proposed)) {
          c.output(toTabDelimited(count, matchRequest, existing, proposed));
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
        SpeciesMatchRequest verbatim,
        GBIFClassification current,
        GBIFClassification proposed) {

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
          verbatim.getVerbatimTaxonRank(),
          verbatim.getScientificName(),
          verbatim.getGenericName(),
          verbatim.getScientificNameAuthorship(),
          current.toString(),
          proposed.toString());
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
