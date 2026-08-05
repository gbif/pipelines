package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MaterialGeologicalContextJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder()
            .appName("MaterialGeologicalContextJoinBuilderTest")
            .getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  private Dataset<Row> occurrenceDf(List<Row> rows) {
    StructType schema = new StructType().add("occurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceWithFormationDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("formation", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialGeoDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("geologicalContext_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> geoDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("geologicalContext_pk", DataTypes.StringType)
            .add("geologicalContextID", DataTypes.StringType)
            .add("formation", DataTypes.StringType)
            .add("bed", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  @Test
  void singleMaterialAndSingleContext_addsGeologicalTerms() {
    Dataset<Row> occurrence = occurrenceDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-001", "OCC001")));
    Dataset<Row> materialGeo = materialGeoDf(List.of(RowFactory.create("MEPK-001", "GCPK-001")));
    Dataset<Row> geo =
        geoDf(List.of(RowFactory.create("GCPK-001", "GEO-001", "Morrison", "Brushy Basin")));

    Dataset<Row> result =
        MaterialGeologicalContextJoinBuilder.enrichOccurrences(
            TestTableLoader.of(
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                MaterialGeologicalContextJoinBuilder.TABLE_MATERIAL_GEOLOGICAL_CONTEXT,
                materialGeo,
                GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT,
                geo,
                "occurrence",
                occurrence),
            occurrence);

    Row row = result.first();
    assertEquals("Morrison", row.getAs("formation"));
    assertEquals("Brushy Basin", row.getAs("bed"));
    assertFalse(Arrays.asList(result.columns()).contains("geologicalContext_pk"));
    assertFalse(Arrays.asList(result.columns()).contains("geologicalContextID"));
  }

  @Test
  void occurrenceValues_takePrecedenceOverMaterialGeologicalContext() {
    Dataset<Row> occurrence =
        occurrenceWithFormationDf(List.of(RowFactory.create("OCC001", "Occurrence formation")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-001", "OCC001")));
    Dataset<Row> materialGeo = materialGeoDf(List.of(RowFactory.create("MEPK-001", "GCPK-001")));
    Dataset<Row> geo =
        geoDf(List.of(RowFactory.create("GCPK-001", "GEO-001", "Material formation", "Bed A")));

    Dataset<Row> result =
        MaterialGeologicalContextJoinBuilder.enrichOccurrences(
            TestTableLoader.of(
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                MaterialGeologicalContextJoinBuilder.TABLE_MATERIAL_GEOLOGICAL_CONTEXT,
                materialGeo,
                GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT,
                geo,
                "occurrence",
                occurrence),
            occurrence);

    assertEquals("Occurrence formation", result.first().getAs("formation"));
    assertEquals("Bed A", result.first().getAs("bed"));
  }

  @Test
  void multipleContextsForMaterial_areNotFlattenedOntoOccurrence() {
    Dataset<Row> occurrence = occurrenceDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-001", "OCC001")));
    Dataset<Row> materialGeo =
        materialGeoDf(
            List.of(
                RowFactory.create("MEPK-001", "GCPK-001"),
                RowFactory.create("MEPK-001", "GCPK-002")));
    Dataset<Row> geo =
        geoDf(
            List.of(
                RowFactory.create("GCPK-001", "GEO-001", "Formation A", "Bed A"),
                RowFactory.create("GCPK-002", "GEO-002", "Formation B", "Bed B")));

    Dataset<Row> result =
        MaterialGeologicalContextJoinBuilder.enrichOccurrences(
            TestTableLoader.of(
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                MaterialGeologicalContextJoinBuilder.TABLE_MATERIAL_GEOLOGICAL_CONTEXT,
                materialGeo,
                GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT,
                geo),
            occurrence);

    assertFalse(Arrays.asList(result.columns()).contains("bed"));
  }

  // ---- computeFunnel ----

  @Test
  void computeFunnel_noSingleMaterialLinks_returnsEmpty() {
    var result = MaterialGeologicalContextJoinBuilder.computeFunnel(TestTableLoader.of());

    assertTrue(result.isEmpty());
  }

  @Test
  void computeFunnel_noMaterialGeoLink_baseAllInNoLinkBucket() {
    Dataset<Row> occurrence = occurrenceDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-001", "OCC001")));
    Dataset<Row> materialGeo = materialGeoDf(List.of());
    Dataset<Row> geo = geoDf(List.of(RowFactory.create("GCPK-001", "GEO-001", "Morrison", "BB")));

    var result =
        MaterialGeologicalContextJoinBuilder.computeFunnel(
            TestTableLoader.of(
                "occurrence",
                occurrence,
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                MaterialGeologicalContextJoinBuilder.TABLE_MATERIAL_GEOLOGICAL_CONTEXT,
                materialGeo,
                GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT,
                geo));

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(5, buckets.size());
    assertEquals(1L, buckets.get(0).count(), "unambiguous single-material links (base)");
    assertEquals(1L, buckets.get(1).count(), "no material-geological-context link");
    assertEquals(0L, buckets.get(2).count(), "resolved, enriched");
    assertEquals(0L, buckets.get(3).count(), "dangling FK, DROPPED");
    assertEquals(0L, buckets.get(4).count(), "ambiguous, DROPPED");
  }

  @Test
  void computeFunnel_fullMixOfAllFiveBuckets() {
    // OCC001: single unambiguous context, resolves -> enriched
    // OCC002: single context link, but the geological-context row doesn't exist -> dangling
    // OCC003: two distinct context links for the same material -> ambiguous, dropped
    Dataset<Row> occurrence =
        occurrenceDf(
            List.of(
                RowFactory.create("OCC001"),
                RowFactory.create("OCC002"),
                RowFactory.create("OCC003")));
    Dataset<Row> material =
        materialDf(
            List.of(
                RowFactory.create("MEPK-1", "OCC001"),
                RowFactory.create("MEPK-2", "OCC002"),
                RowFactory.create("MEPK-3", "OCC003")));
    Dataset<Row> materialGeo =
        materialGeoDf(
            List.of(
                RowFactory.create("MEPK-1", "GCPK-1"),
                RowFactory.create("MEPK-2", "GCPK-UNKNOWN"),
                RowFactory.create("MEPK-3", "GCPK-1"),
                RowFactory.create("MEPK-3", "GCPK-2")));
    Dataset<Row> geo =
        geoDf(
            List.of(
                RowFactory.create("GCPK-1", "GEO-1", "Morrison", "Brushy Basin"),
                RowFactory.create("GCPK-2", "GEO-2", "Formation B", "Bed B")));

    var result =
        MaterialGeologicalContextJoinBuilder.computeFunnel(
            TestTableLoader.of(
                "occurrence",
                occurrence,
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                MaterialGeologicalContextJoinBuilder.TABLE_MATERIAL_GEOLOGICAL_CONTEXT,
                materialGeo,
                GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT,
                geo));

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(5, buckets.size());
    assertEquals(3L, buckets.get(0).count(), "unambiguous single-material links (base)");
    assertEquals(0L, buckets.get(1).count(), "no material-geological-context link");
    assertEquals(1L, buckets.get(2).count(), "resolved, enriched — OCC001");
    assertEquals(1L, buckets.get(3).count(), "dangling FK, DROPPED — OCC002");
    assertEquals(1L, buckets.get(4).count(), "ambiguous, DROPPED — OCC003");
  }
}
