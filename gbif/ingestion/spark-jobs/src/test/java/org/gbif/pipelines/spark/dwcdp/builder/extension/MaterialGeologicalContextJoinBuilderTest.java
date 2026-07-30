package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
                geo),
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
                geo),
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
}
