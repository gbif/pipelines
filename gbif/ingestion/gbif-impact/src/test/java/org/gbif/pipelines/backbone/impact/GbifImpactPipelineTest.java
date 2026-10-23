package org.gbif.pipelines.backbone.impact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.species.Metadata;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchResponse.RankedName;
import org.gbif.rest.client.species.NameUsageMatchResponse.Usage;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GbifImpactPipelineTest {

  private static SparkSession spark;

  @TempDir Path tmp;

  @BeforeAll
  static void setUp() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("gbif-impact-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  /** Puma concolor stays put, Felis catus moves to a new genus, Rare taxon is under the limit. */
  static class StubService implements NameUsageMatchingService {
    @Override
    public NameUsageMatchResponse match(NameUsageMatchRequest request) {
      NameUsageMatchResponse r = new NameUsageMatchResponse();
      if ("Puma concolor".equals(request.getScientificName())) {
        r.setUsage(usage("2435099", "Puma concolor"));
        r.setClassification(
            Arrays.asList(
                new RankedName("1", "Animalia", "KINGDOM", null),
                new RankedName("2435098", "Puma", "GENUS", null),
                new RankedName("2435099", "Puma concolor", "SPECIES", null)));
      } else if ("Felis catus".equals(request.getScientificName())) {
        r.setUsage(usage("999", "Newgenus catus"));
        r.setClassification(
            Arrays.asList(
                new RankedName("1", "Animalia", "KINGDOM", null),
                new RankedName("998", "Newgenus", "GENUS", null),
                new RankedName("999", "Newgenus catus", "SPECIES", null)));
      }
      return r;
    }

    @Override
    public Metadata getMetadata(String checklistKey) {
      return null;
    }

    private static Usage usage(String key, String name) {
      Usage u = new Usage();
      u.setKey(key);
      u.setName(name);
      return u;
    }
  }

  @Test
  void reportsChangedClassifications() throws Exception {
    String sourcePath = tmp.resolve("source").toString();
    writeSource(sourcePath);

    GbifImpactArgs args = new GbifImpactArgs();
    args.sourcePath = sourcePath;
    args.targetDir = tmp.resolve("out").toString();
    args.apiBaseUri = "http://api.example.org/v1";
    args.checklistKey = "test";
    args.minimumOccurrenceCount = 10;
    args.skipKeys = true;
    args.numPartitions = 2;

    GbifImpactPipeline.run(spark, args, StubService::new);

    // parquet output
    Dataset<Row> impact = spark.read().parquet(args.targetDir + "/impact");
    List<Row> rows = impact.collectAsList();
    assertEquals(1, rows.size());
    Row felis = rows.get(0);
    assertEquals(500L, (long) felis.getAs("count"));
    assertEquals("Felis catus", felis.getAs("verbatim_scientificName"));
    assertEquals("Felis", felis.getAs("current_genus"));
    assertEquals("Newgenus", felis.getAs("proposed_genus"));
    assertTrue(felis.<String>getAs("debug_url").contains("scientificName=Felis+catus"));
    assertFalse(Arrays.asList(impact.columns()).contains("current_kingdomKey"));

    // tsv report
    List<String> lines = Files.readAllLines(tmp.resolve("out").resolve("report.csv"));
    assertEquals(2, lines.size());
    assertEquals(String.join("\t", ImpactMatcher.headers(true)), lines.get(0));
    assertTrue(lines.get(1).startsWith("500\t"));
    assertFalse(new File(args.targetDir, "_report_tmp").exists());
  }

  @Test
  void scopeLimitsRecords() throws Exception {
    String sourcePath = tmp.resolve("source").toString();
    writeSource(sourcePath);

    GbifImpactArgs args = new GbifImpactArgs();
    args.sourcePath = sourcePath;
    args.targetDir = tmp.resolve("out").toString();
    args.apiBaseUri = "http://api.example.org/v1";
    args.scope = 2435098; // Puma
    args.skipKeys = true;

    GbifImpactPipeline.run(spark, args, StubService::new);

    assertEquals(0, spark.read().parquet(args.targetDir + "/impact").count());
    List<String> lines = Files.readAllLines(tmp.resolve("out").resolve("report.csv"));
    assertTrue(lines.size() <= 1);
  }

  @Test
  void keysAreComparedWhenNotSkipped() throws Exception {
    String sourcePath = tmp.resolve("source").toString();
    writeSource(sourcePath);

    GbifImpactArgs args = new GbifImpactArgs();
    args.sourcePath = sourcePath;
    args.targetDir = tmp.resolve("out").toString();
    args.apiBaseUri = "http://api.example.org/v1";
    args.minimumOccurrenceCount = 10;
    args.skipKeys = false;

    GbifImpactPipeline.run(spark, args, StubService::new);

    List<String> names =
        spark.read().parquet(args.targetDir + "/impact").collectAsList().stream()
            .map(r -> r.<String>getAs("verbatim_scientificName"))
            .sorted()
            .collect(Collectors.toList());
    // Puma has identical names and keys (nulls on both sides), so only Felis is reported
    assertEquals(List.of("Felis catus"), names);
  }

  /** Writes a source table in the shape described in the project readme. */
  private static void writeSource(String path) {
    String[] stringCols = {
      "v_kingdom",
      "v_phylum",
      "v_class",
      "v_order",
      "v_superfamily",
      "v_family",
      "v_subfamily",
      "v_tribe",
      "v_subtribe",
      "v_genus",
      "v_subGenus",
      "v_scientificName",
      "v_scientificNameAuthorship",
      "v_taxonRank",
      "v_verbatimTaxonRank",
      "v_genericName",
      "v_specificEpithet",
      "v_infraSpecificEpithet",
      "v_cultivarEpithet",
      "v_scientificNameID",
      "v_taxonID",
      "v_taxonConceptID",
      "kingdom",
      "phylum",
      "class",
      "order",
      "superfamily",
      "family",
      "subfamily",
      "tribe",
      "subtribe",
      "genus",
      "subGenus",
      "species",
      "scientificName",
      "acceptedScientificName"
    };
    String[] intCols = {
      "kingdomKey",
      "phylumKey",
      "classKey",
      "orderKey",
      "familyKey",
      "genusKey",
      "subGenusKey",
      "speciesKey",
      "taxonKey",
      "acceptedTaxonKey"
    };

    StructField[] fields = new StructField[stringCols.length + intCols.length + 1];
    int i = 0;
    for (String c : stringCols) {
      fields[i++] = DataTypes.createStructField(c, DataTypes.StringType, true);
    }
    for (String c : intCols) {
      fields[i++] = DataTypes.createStructField(c, DataTypes.IntegerType, true);
    }
    fields[i] = DataTypes.createStructField("occurrenceCount", DataTypes.LongType, false);
    StructType schema = DataTypes.createStructType(fields);

    List<Row> rows =
        List.of(
            row(schema, "Puma concolor", "Puma", "Puma concolor", 2435098, 2435099, 1000L),
            row(schema, "Felis catus", "Felis", "Felis catus", 2435022, 2435035, 500L),
            row(schema, "Rare taxon", "Rare", "Rare taxon", 5, 6, 3L));

    spark.createDataFrame(rows, schema).write().mode("overwrite").parquet(path);
  }

  private static Row row(
      StructType schema,
      String name,
      String genus,
      String species,
      int genusKey,
      int speciesKey,
      long count) {
    Object[] values = new Object[schema.fields().length];
    values[schema.fieldIndex("v_kingdom")] = "Animalia";
    values[schema.fieldIndex("v_scientificName")] = name;
    values[schema.fieldIndex("kingdom")] = "Animalia";
    values[schema.fieldIndex("genus")] = genus;
    values[schema.fieldIndex("species")] = species;
    values[schema.fieldIndex("scientificName")] = species;
    values[schema.fieldIndex("acceptedScientificName")] = species;
    values[schema.fieldIndex("kingdomKey")] = 1;
    values[schema.fieldIndex("genusKey")] = genusKey;
    values[schema.fieldIndex("speciesKey")] = speciesKey;
    values[schema.fieldIndex("taxonKey")] = speciesKey;
    values[schema.fieldIndex("acceptedTaxonKey")] = speciesKey;
    values[schema.fieldIndex("occurrenceCount")] = count;
    return RowFactory.create(values);
  }
}
