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

/**
 * Fixture convention: {@code provenance_fk} values (direct on {@code material} and on the {@code
 * material-provenance} junction table) match a {@code provenance_pk} (surrogate) value, never a
 * {@code provenanceID} (natural) one — same convention as {@link ProvenanceJoinBuilderTest}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MaterialProvenanceJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder().appName("MaterialProvenanceJoinBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> occurrenceDf(List<Row> rows) {
    StructType schema = new StructType().add("occurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialWithDirectFkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialNoProvenanceFkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialProvenanceJunctionDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> provenanceDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("provenance_pk", DataTypes.StringType)
            .add("provenanceID", DataTypes.StringType)
            .add("fundingAttribution", DataTypes.StringType)
            .add("fundingAttributionID", DataTypes.StringType)
            .add("projectID", DataTypes.StringType)
            .add("projectTitle", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- absent-table cases ----

  @Test
  void provenanceTableAbsent_returnsOccurrenceDfUnchanged() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material =
        materialWithDirectFkDf(List.of(RowFactory.create("MEPK-1", "OCC001", "PPK-1")));

    Dataset<Row> result =
        MaterialProvenanceJoinBuilder.enrichOccurrences(
            TestTableLoader.of(MaterialJoinBuilder.TABLE_MATERIAL, material), occ);

    assertEquals(occ.columns().length, result.columns().length);
  }

  @Test
  void noMaterialAtAll_returnsOccurrenceDfUnchanged() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Project A")));

    Dataset<Row> result =
        MaterialProvenanceJoinBuilder.enrichOccurrences(
            TestTableLoader.of(ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance), occ);

    assertEquals(occ.columns().length, result.columns().length);
  }

  // ---- direct provenance_fk on material ----

  @Test
  void directProvenanceFk_fieldsAggregatedOntoOccurrence() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material =
        materialWithDirectFkDf(List.of(RowFactory.create("MEPK-1", "OCC001", "PPK-1")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create(
                    "PPK-1", "PROV-1", "NSF Grant 123", "FID-1", "PID-1", "Digitization Project")));

    Dataset<Row> result =
        MaterialProvenanceJoinBuilder.enrichOccurrences(
            TestTableLoader.of(
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                ProvenanceJoinBuilder.TABLE_PROVENANCE,
                provenance,
                "occurrence",
                occ),
            occ);

    Row row = result.first();
    assertEquals("NSF Grant 123", row.getAs("fundingAttribution"));
    assertEquals("Digitization Project", row.getAs("projectTitle"));
  }

  @Test
  void materialEntityPkSurrogate_neverLeaksIntoResult() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material =
        materialWithDirectFkDf(List.of(RowFactory.create("MEPK-1", "OCC001", "PPK-1")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Project A")));

    Dataset<Row> result =
        MaterialProvenanceJoinBuilder.enrichOccurrences(
            TestTableLoader.of(
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                ProvenanceJoinBuilder.TABLE_PROVENANCE,
                provenance,
                "occurrence",
                occ),
            occ);

    assertFalse(Arrays.asList(result.columns()).contains("materialEntity_pk"));
  }

  // ---- junction table ----

  @Test
  void junctionTable_multipleLinkedProvenance_sortedByProvenanceId() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material =
        materialNoProvenanceFkDf(List.of(RowFactory.create("MEPK-1", "OCC001")));
    Dataset<Row> junction =
        materialProvenanceJunctionDf(
            List.of(RowFactory.create("MEPK-1", "PPK-2"), RowFactory.create("MEPK-1", "PPK-1")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create("PPK-2", "PROV-2", "Grant B", "FID-B", "PID-B", "Project B"),
                RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Project A")));

    Dataset<Row> result =
        MaterialProvenanceJoinBuilder.enrichOccurrences(
            TestTableLoader.of(
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                MaterialProvenanceJoinBuilder.TABLE_MATERIAL_PROVENANCE,
                junction,
                ProvenanceJoinBuilder.TABLE_PROVENANCE,
                provenance,
                "occurrence",
                occ),
            occ);

    Row row = result.first();
    assertEquals("Grant A|Grant B", row.getAs("fundingAttribution"));
  }

  // ---- exactly-one-material rule ----

  @Test
  void ambiguousMaterial_occurrenceNotEnriched() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material =
        materialWithDirectFkDf(
            List.of(
                RowFactory.create("MEPK-1", "OCC001", "PPK-1"),
                RowFactory.create("MEPK-2", "OCC001", "PPK-1")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Project A")));

    Dataset<Row> result =
        MaterialProvenanceJoinBuilder.enrichOccurrences(
            TestTableLoader.of(
                MaterialJoinBuilder.TABLE_MATERIAL, material,
                ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance),
            occ);

    assertEquals(1L, result.count(), "occurrence must survive even though it isn't enriched");
    assertFalse(
        Arrays.asList(result.columns()).contains("fundingAttribution"),
        "two material rows citing the same occurrence must not be guessed at");
  }

  // ---- occurrence survives with no matching material provenance ----

  @Test
  void occurrenceSurvivesWithNoMatchingMaterialProvenance() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material =
        materialWithDirectFkDf(List.of(RowFactory.create("MEPK-1", "OCC999", "PPK-1")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Project A")));

    Dataset<Row> result =
        MaterialProvenanceJoinBuilder.enrichOccurrences(
            TestTableLoader.of(
                MaterialJoinBuilder.TABLE_MATERIAL, material,
                ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance),
            occ);

    assertEquals(1L, result.count());
    assertEquals("OCC001", result.first().getAs("occurrenceID"));
  }
}
