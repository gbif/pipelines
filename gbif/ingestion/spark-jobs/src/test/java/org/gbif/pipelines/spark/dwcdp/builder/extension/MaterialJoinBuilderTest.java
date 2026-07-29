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
class MaterialJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("MaterialJoinBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> occurrenceDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceNoIdDf(List<Row> rows) {
    StructType schema = new StructType().add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("institutionCode", DataTypes.StringType)
            .add("collectionCode", DataTypes.StringType)
            .add("catalogNumber", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- absent-table / absent-column cases ----

  @Test
  void materialTableAbsent_returnsOccurrenceDfUnchanged() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001", "Parus major")));

    Dataset<Row> result = MaterialJoinBuilder.enrichOccurrences(TestTableLoader.of(), occ);

    assertEquals(occ.columns().length, result.columns().length);
  }

  @Test
  void occurrenceWithoutOccurrenceIdColumn_returnsUnchanged() {
    Dataset<Row> occ = occurrenceNoIdDf(List.of(RowFactory.create("Parus major")));
    Dataset<Row> material =
        materialDf(
            List.of(RowFactory.create("MEPK-1", "OCC001", "NHMD", "AVES", "12345", "Parus major")));

    Dataset<Row> result =
        MaterialJoinBuilder.enrichOccurrences(
            TestTableLoader.of(MaterialJoinBuilder.TABLE_MATERIAL, material), occ);

    assertEquals(1, result.columns().length, "should be original columns only");
  }

  @Test
  void materialMissingEvidenceColumn_returnsUnchanged() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001", "Parus major")));
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("institutionCode", DataTypes.StringType);
    Dataset<Row> material =
        spark.createDataFrame(List.of(RowFactory.create("MEPK-1", "NHMD")), schema);

    Dataset<Row> result =
        MaterialJoinBuilder.enrichOccurrences(
            TestTableLoader.of(MaterialJoinBuilder.TABLE_MATERIAL, material), occ);

    assertEquals(2, result.columns().length, "should be original occurrence columns only");
  }

  // ---- exactly one material ----

  @Test
  void exactlyOneMaterial_institutionAndCatalogFieldsAdded() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001", "Parus major")));
    Dataset<Row> material =
        materialDf(
            List.of(RowFactory.create("MEPK-1", "OCC001", "NHMD", "AVES", "12345", "Parus major")));

    Dataset<Row> result =
        MaterialJoinBuilder.enrichOccurrences(
            TestTableLoader.of(MaterialJoinBuilder.TABLE_MATERIAL, material), occ);

    Row row = result.first();
    assertEquals("NHMD", row.getAs("institutionCode"));
    assertEquals("AVES", row.getAs("collectionCode"));
    assertEquals("12345", row.getAs("catalogNumber"));
  }

  @Test
  void exactlyOneMaterial_occurrenceScientificNameWinsOverMaterialCopy() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001", "Parus major (occ)")));
    Dataset<Row> material =
        materialDf(
            List.of(
                RowFactory.create(
                    "MEPK-1",
                    "OCC001",
                    "NHMD",
                    "AVES",
                    "12345",
                    "Parus major (material, should not win)")));

    Dataset<Row> result =
        MaterialJoinBuilder.enrichOccurrences(
            TestTableLoader.of(MaterialJoinBuilder.TABLE_MATERIAL, material), occ);

    Row row = result.first();
    assertEquals(
        "Parus major (occ)",
        row.getAs("scientificName"),
        "occurrence's own scientificName must win over material's copy");
  }

  @Test
  void surrogateKeysAndEventFksNeverLeakIntoResult() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001", "Parus major")));
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("collectionEvent_fk", DataTypes.StringType)
            .add("derivationEvent_fk", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("institutionCode", DataTypes.StringType);
    Dataset<Row> material =
        spark.createDataFrame(
            List.of(
                RowFactory.create(
                    "MEPK-1", "OCC001", "EPK-1", "EPK-2", "PROVPK-1", "UPPK-1", "NHMD")),
            schema);

    Dataset<Row> result =
        MaterialJoinBuilder.enrichOccurrences(
            TestTableLoader.of(MaterialJoinBuilder.TABLE_MATERIAL, material), occ);

    List<String> cols = Arrays.asList(result.columns());
    assertFalse(cols.contains("materialEntity_pk"));
    assertFalse(cols.contains("evidenceForOccurrenceID"));
    assertFalse(cols.contains("collectionEvent_fk"));
    assertFalse(cols.contains("derivationEvent_fk"));
    assertFalse(cols.contains("provenance_fk"));
    assertFalse(cols.contains("usagePolicy_fk"));
    assertTrue(cols.contains("institutionCode"), "genuine material fields must still come through");
  }

  // ---- zero material ----

  @Test
  void occurrenceWithNoMaterialRowsAtAll_survivesUnenriched() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001", "Parus major")));
    Dataset<Row> material =
        materialDf(
            List.of(RowFactory.create("MEPK-1", "OCC999", "NHMD", "AVES", "99999", "Other sp.")));

    Dataset<Row> result =
        MaterialJoinBuilder.enrichOccurrences(
            TestTableLoader.of(MaterialJoinBuilder.TABLE_MATERIAL, material), occ);

    assertEquals(1L, result.count(), "occurrence must survive even with no matching material");
    Row row = result.first();
    assertEquals("OCC001", row.getAs("occurrenceID"));
    assertEquals("Parus major", row.getAs("scientificName"));
  }

  // ---- more than one material: voucher + tissue sample, a real valid scenario ----

  @Test
  void multipleMaterialRowsForSameOccurrence_occurrenceUnenriched() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001", "Parus major")));
    Dataset<Row> material =
        materialDf(
            List.of(
                // voucher specimen
                RowFactory.create("MEPK-1", "OCC001", "NHMD", "AVES", "12345", "Parus major"),
                // separately accessioned tissue sample, same occurrence
                RowFactory.create("MEPK-2", "OCC001", "NHMD", "TISSUE", "T-9999", "Parus major")));

    Dataset<Row> result =
        MaterialJoinBuilder.enrichOccurrences(
            TestTableLoader.of(MaterialJoinBuilder.TABLE_MATERIAL, material), occ);

    assertEquals(1L, result.count());
    Row row = result.first();
    assertTrue(
        row.isNullAt(row.fieldIndex("institutionCode")),
        "two material rows citing the same occurrence must not be guessed at — occurrence stays "
            + "unenriched (institutionCode present as a column but null-valued)");
  }

  // ---- multiple occurrences, mixed cases ----

  @Test
  void multipleOccurrences_eachEvaluatedIndependently() {
    Dataset<Row> occ =
        occurrenceDf(
            List.of(
                RowFactory.create("OCC001", "Parus major"),
                RowFactory.create("OCC002", "Quercus robur")));
    Dataset<Row> material =
        materialDf(
            List.of(
                // OCC001: exactly one -> enriched
                RowFactory.create("MEPK-1", "OCC001", "NHMD", "AVES", "12345", "Parus major"),
                // OCC002: two -> unenriched
                RowFactory.create("MEPK-2", "OCC002", "NHMD", "BOT", "B-1", "Quercus robur"),
                RowFactory.create("MEPK-3", "OCC002", "NHMD", "BOT", "B-2", "Quercus robur")));

    Dataset<Row> result =
        MaterialJoinBuilder.enrichOccurrences(
            TestTableLoader.of(MaterialJoinBuilder.TABLE_MATERIAL, material), occ);

    assertEquals(2L, result.count());

    Row occ001 = result.filter(result.col("occurrenceID").equalTo("OCC001")).first();
    assertEquals("NHMD", occ001.getAs("institutionCode"));

    Row occ002 = result.filter(result.col("occurrenceID").equalTo("OCC002")).first();
    assertTrue(
        occ002.isNullAt(occ002.fieldIndex("institutionCode")),
        "OCC002 has two material rows, so it must not be enriched");
  }

  // ---- usage-policy enrichment flows through onto occurrence ----

  @Test
  void materialUsagePolicy_licenseAndRightsHolderFlowThroughOntoOccurrence() {
    Dataset<Row> occ = occurrenceDf(List.of(RowFactory.create("OCC001", "Parus major")));
    Dataset<Row> material =
        materialDf(
            List.of(RowFactory.create("MEPK-1", "OCC001", "NHMD", "AVES", "12345", "Parus major")));
    StructType materialWithUsagePolicySchema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("institutionCode", DataTypes.StringType);
    Dataset<Row> materialWithUsagePolicy =
        spark.createDataFrame(
            List.of(RowFactory.create("MEPK-1", "OCC001", "UP-1", "NHMD")),
            materialWithUsagePolicySchema);
    StructType usagePolicySchema =
        new StructType()
            .add("usagePolicy_pk", DataTypes.StringType)
            .add("license", DataTypes.StringType)
            .add("rightsHolder", DataTypes.StringType);
    Dataset<Row> usagePolicy =
        spark.createDataFrame(
            List.of(RowFactory.create("UP-1", "CC_BY_4_0", "Natural History Museum Denmark")),
            usagePolicySchema);

    Dataset<Row> result =
        MaterialJoinBuilder.enrichOccurrences(
            TestTableLoader.of(
                MaterialJoinBuilder.TABLE_MATERIAL, materialWithUsagePolicy,
                UsagePolicyJoinBuilder.TABLE_USAGE_POLICY, usagePolicy),
            occ);

    Row row = result.first();
    assertEquals("CC_BY_4_0", row.getAs("license"));
    assertEquals("Natural History Museum Denmark", row.getAs("rightsHolder"));
    assertFalse(
        Arrays.asList(result.columns()).contains("usagePolicy_fk"),
        "the join FK must not survive, same as every other surrogate key");
  }
}
