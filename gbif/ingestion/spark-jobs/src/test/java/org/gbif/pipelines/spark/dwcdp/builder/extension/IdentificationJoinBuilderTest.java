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
class IdentificationJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("IdentificationJoinBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> occurrencePkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceNoPkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> identificationDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("identification_pk", DataTypes.StringType)
            .add("occurrence_fk", DataTypes.StringType)
            .add("isAcceptedIdentification", DataTypes.BooleanType)
            .add("scientificName", DataTypes.StringType)
            .add("kingdom", DataTypes.StringType)
            .add("family", DataTypes.StringType)
            .add("genus", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- absent-table / absent-column cases ----

  @Test
  void identificationTableAbsent_returnsOccurrenceDfUnchanged() {
    Dataset<Row> occ =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "Parus major")));

    Dataset<Row> result = IdentificationJoinBuilder.enrichOccurrences(TestTableLoader.of(), occ);

    assertEquals(occ.columns().length, result.columns().length);
  }

  @Test
  void occurrenceWithoutOccurrencePkColumn_returnsUnchanged() {
    Dataset<Row> occ = occurrenceNoPkDf(List.of(RowFactory.create("OCC001", "Parus major")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1", "OPK-001", true, "Parus major", "Animalia", "Paridae", "Parus")));

    Dataset<Row> result =
        IdentificationJoinBuilder.enrichOccurrences(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identDf), occ);

    assertEquals(2, result.columns().length, "should be original columns only");
  }

  @Test
  void identificationMissingRequiredColumns_returnsUnchanged() {
    Dataset<Row> occ =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "Parus major")));
    StructType schema =
        new StructType()
            .add("identification_pk", DataTypes.StringType)
            .add("kingdom", DataTypes.StringType);
    Dataset<Row> identDf =
        spark.createDataFrame(List.of(RowFactory.create("IDPK-1", "Animalia")), schema);

    Dataset<Row> result =
        IdentificationJoinBuilder.enrichOccurrences(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identDf), occ);

    assertEquals(3, result.columns().length, "should be original occurrence columns only");
  }

  // ---- exactly one accepted identification ----

  @Test
  void exactlyOneAccepted_rankHierarchyAdded() {
    Dataset<Row> occ =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "Parus major")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1", "OPK-001", true, "Parus major", "Animalia", "Paridae", "Parus")));

    Dataset<Row> result =
        IdentificationJoinBuilder.enrichOccurrences(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identDf), occ);

    Row row = result.filter(result.col("occurrenceID").equalTo("OCC001")).first();
    assertEquals("Animalia", row.getAs("kingdom"));
    assertEquals("Paridae", row.getAs("family"));
    assertEquals("Parus", row.getAs("genus"));
  }

  @Test
  void exactlyOneAccepted_occurrenceScientificNameWinsOverIdentificationCopy() {
    Dataset<Row> occ =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "Parus major (occ)")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1",
                    "OPK-001",
                    true,
                    "Parus major (ident, should not win)",
                    "Animalia",
                    "Paridae",
                    "Parus")));

    Dataset<Row> result =
        IdentificationJoinBuilder.enrichOccurrences(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identDf), occ);

    Row row = result.first();
    assertEquals(
        "Parus major (occ)",
        row.getAs("scientificName"),
        "occurrence's own scientificName must win over identification's copy");
  }

  @Test
  void surrogateKeysAndFilterColumn_neverLeakIntoResult() {
    Dataset<Row> occ =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "Parus major")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1", "OPK-001", true, "Parus major", "Animalia", "Paridae", "Parus")));

    Dataset<Row> result =
        IdentificationJoinBuilder.enrichOccurrences(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identDf), occ);

    List<String> cols = Arrays.asList(result.columns());
    assertFalse(cols.contains("identification_pk"));
    assertFalse(cols.contains("occurrence_fk"));
    assertFalse(cols.contains("isAcceptedIdentification"));
  }

  // ---- zero accepted ----

  @Test
  void noAcceptedIdentification_occurrenceUnenriched() {
    Dataset<Row> occ =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "Parus major")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1", "OPK-001", false, "Parus major", "Animalia", "Paridae", "Parus")));

    Dataset<Row> result =
        IdentificationJoinBuilder.enrichOccurrences(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identDf), occ);

    Row row = result.first();
    assertTrue(
        row.isNullAt(row.fieldIndex("kingdom")),
        "no accepted identification means the rank hierarchy value stays null for this occurrence");
  }

  @Test
  void occurrenceWithNoIdentificationRowsAtAll_survivesUnenriched() {
    Dataset<Row> occ =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "Parus major")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1", "OPK-999", true, "Quercus robur", "Plantae", "Fagaceae", "Quercus")));

    Dataset<Row> result =
        IdentificationJoinBuilder.enrichOccurrences(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identDf), occ);

    assertEquals(
        1L, result.count(), "occurrence must survive even with no matching identification");
    Row row = result.first();
    assertEquals("OCC001", row.getAs("occurrenceID"));
    assertEquals("Parus major", row.getAs("scientificName"));
  }

  // ---- more than one accepted ----

  @Test
  void multipleAcceptedIdentifications_occurrenceUnenriched() {
    Dataset<Row> occ =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "Parus major")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1", "OPK-001", true, "Parus major", "Animalia", "Paridae", "Parus"),
                RowFactory.create(
                    "IDPK-2",
                    "OPK-001",
                    true,
                    "Parus major major",
                    "Animalia",
                    "Paridae",
                    "Parus")));

    Dataset<Row> result =
        IdentificationJoinBuilder.enrichOccurrences(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identDf), occ);

    assertEquals(1L, result.count());
    Row row = result.first();
    assertTrue(
        row.isNullAt(row.fieldIndex("kingdom")),
        "two accepted identifications for the same occurrence must not be guessed at — occurrence "
            + "stays unenriched (kingdom present as a column but null-valued)");
  }

  // ---- multiple occurrences, mixed cases ----

  @Test
  void multipleOccurrences_eachEvaluatedIndependently() {
    Dataset<Row> occ =
        occurrencePkDf(
            List.of(
                RowFactory.create("OPK-001", "OCC001", "Parus major"),
                RowFactory.create("OPK-002", "OCC002", "Quercus robur")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                // OCC001: exactly one accepted -> enriched
                RowFactory.create(
                    "IDPK-1", "OPK-001", true, "Parus major", "Animalia", "Paridae", "Parus"),
                // OCC002: two accepted -> unenriched
                RowFactory.create(
                    "IDPK-2", "OPK-002", true, "Quercus robur", "Plantae", "Fagaceae", "Quercus"),
                RowFactory.create(
                    "IDPK-3",
                    "OPK-002",
                    true,
                    "Quercus robur var. x",
                    "Plantae",
                    "Fagaceae",
                    "Quercus")));

    Dataset<Row> result =
        IdentificationJoinBuilder.enrichOccurrences(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identDf), occ);

    assertEquals(2L, result.count());

    Row occ001 = result.filter(result.col("occurrenceID").equalTo("OCC001")).first();
    assertEquals("Animalia", occ001.getAs("kingdom"));

    Row occ002 = result.filter(result.col("occurrenceID").equalTo("OCC002")).first();
    assertTrue(
        occ002.isNullAt(occ002.fieldIndex("kingdom")),
        "OCC002 has two accepted identifications, so it must not be enriched");
  }

  // ---- computeFunnel ----

  @Test
  void computeFunnel_identificationTableAbsent_returnsEmpty() {
    var result = IdentificationJoinBuilder.computeFunnel(TestTableLoader.of());

    assertTrue(result.isEmpty());
  }

  @Test
  void computeFunnel_requiredColumnsAbsent_returnsEmpty() {
    StructType schema = new StructType().add("identification_pk", DataTypes.StringType);
    Dataset<Row> identification =
        spark.createDataFrame(List.of(RowFactory.create("IPK-1")), schema);

    var result =
        IdentificationJoinBuilder.computeFunnel(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identification));

    assertTrue(result.isEmpty());
  }

  @Test
  void computeFunnel_fullMixOfAllFiveBuckets() {
    Dataset<Row> identification =
        identificationDf(
            List.of(
                // OCC001: single accepted -> used for enrichment
                RowFactory.create("IPK-1", "OCC001", true, "Parus major", "Animalia", "F", "G"),
                // OCC002: two accepted -> ambiguous, dropped (both rows)
                RowFactory.create("IPK-2", "OCC002", true, "Turdus merula", "Animalia", "F", "G"),
                RowFactory.create("IPK-3", "OCC002", true, "Turdus iliacus", "Animalia", "F", "G"),
                // not accepted -> ignored
                RowFactory.create("IPK-4", "OCC003", false, "Corvus corax", "Animalia", "F", "G"),
                // accepted, no occurrence_fk -> ignored
                RowFactory.create("IPK-5", null, true, "Pica pica", "Animalia", "F", "G")));

    var result =
        IdentificationJoinBuilder.computeFunnel(
            TestTableLoader.of(IdentificationJoinBuilder.TABLE_IDENTIFICATION, identification));

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(5, buckets.size());
    assertEquals(5L, buckets.get(0).count(), "identification rows (total)");
    assertEquals(1L, buckets.get(1).count(), "not accepted, ignored — IPK-4");
    assertEquals(1L, buckets.get(2).count(), "accepted, no occurrence_fk, ignored — IPK-5");
    assertEquals(1L, buckets.get(3).count(), "used for enrichment — IPK-1 (OCC001)");
    assertEquals(2L, buckets.get(4).count(), "ambiguous, DROPPED — IPK-2 and IPK-3 (OCC002)");
  }
}
