package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IdentificationExtensionBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder()
            .appName("IdentificationExtensionBuilderTest")
            .getOrCreate();
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
            .add("occurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> identificationDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("identification_pk", DataTypes.StringType)
            .add("occurrence_fk", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
            .add("isAcceptedIdentification", DataTypes.BooleanType)
            .add("identifiedBy", DataTypes.StringType)
            .add("dateIdentified", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- absent-table / absent-column cases ----

  @Test
  void identificationTableAbsent_returnsEmpty() {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));

    Optional<Dataset<Row>> result =
        IdentificationExtensionBuilder.build(spark, TestTableLoader.of("occurrence", occ));

    assertTrue(result.isEmpty());
  }

  @Test
  void occurrenceTableAbsent_returnsEmpty() {
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1", "OPK-001", null, true, "J. Smith", "2020-01-01", "Parus major")));

    Optional<Dataset<Row>> result =
        IdentificationExtensionBuilder.build(
            spark,
            TestTableLoader.of(IdentificationExtensionBuilder.TABLE_IDENTIFICATION, identDf));

    assertTrue(result.isEmpty());
  }

  @Test
  void identificationMissingOccurrenceFkColumn_returnsEmpty() {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    StructType schema =
        new StructType()
            .add("identification_pk", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    Dataset<Row> identDf =
        spark.createDataFrame(List.of(RowFactory.create("IDPK-1", "Parus major")), schema);

    Optional<Dataset<Row>> result =
        IdentificationExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "occurrence", occ, IdentificationExtensionBuilder.TABLE_IDENTIFICATION, identDf));

    assertTrue(result.isEmpty());
  }

  // ---- full history aggregation ----

  @Test
  void allLinkedIdentifications_includedRegardlessOfAcceptedStatus() throws Exception {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                // historical, superseded
                RowFactory.create(
                    "IDPK-1",
                    "OPK-001",
                    null,
                    false,
                    "J. Smith",
                    "2018-01-01",
                    "Parus major (old)"),
                // current, accepted
                RowFactory.create(
                    "IDPK-2", "OPK-001", null, true, "A. Jones", "2020-01-01", "Parus major")));

    Optional<Dataset<Row>> resultOpt =
        IdentificationExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "occurrence", occ, IdentificationExtensionBuilder.TABLE_IDENTIFICATION, identDf));

    assertTrue(resultOpt.isPresent());
    Row row = resultOpt.get().collectAsList().get(0);
    assertEquals("OCC001", row.getAs("occurrenceID"));

    List<Map<String, String>> history = parseHistoryJson(row);
    assertEquals(
        2, history.size(), "both the historical and the current identification must be present");
  }

  @Test
  void surrogateKeysAndOtherEntityFksNeverLeakIntoResult() throws Exception {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1", "OPK-001", "MEPK-1", true, "A. Jones", "2020-01-01", "Parus major")));

    Optional<Dataset<Row>> resultOpt =
        IdentificationExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "occurrence", occ, IdentificationExtensionBuilder.TABLE_IDENTIFICATION, identDf));

    assertTrue(resultOpt.isPresent());
    Row row = resultOpt.get().collectAsList().get(0);
    List<Map<String, String>> history = parseHistoryJson(row);
    assertEquals(1, history.size());

    Map<String, String> entry = history.get(0);
    assertFalse(entry.containsKey("identification_pk"));
    assertFalse(entry.containsKey("occurrence_fk"));
    assertFalse(entry.containsKey("materialEntity_fk"));
    assertNotNull(entry.get(DwcTerm.identifiedBy.qualifiedName()));
  }

  @Test
  void identificationWithoutOccurrenceLink_excludedFromHistory() {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    // This identification is about a material entity, not this (or any) occurrence directly —
    // occurrence_fk is null, so it must not appear anywhere in the occurrence-scoped history.
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1", null, "MEPK-1", true, "A. Jones", "2020-01-01", "Parus major")));

    Optional<Dataset<Row>> result =
        IdentificationExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "occurrence", occ, IdentificationExtensionBuilder.TABLE_IDENTIFICATION, identDf));

    // No identification row has a non-null occurrence_fk, so there's nothing to aggregate —
    // the resulting Dataset (if present at all) must have zero rows.
    if (result.isPresent()) {
      assertEquals(0L, result.get().count());
    }
  }

  // ---- multiple occurrences ----

  @Test
  void multipleOccurrences_eachGetsOwnHistoryIndependently() {
    Dataset<Row> occ =
        occurrencePkDf(
            List.of(
                RowFactory.create("OPK-001", "OCC001"), RowFactory.create("OPK-002", "OCC002")));
    Dataset<Row> identDf =
        identificationDf(
            List.of(
                RowFactory.create(
                    "IDPK-1", "OPK-001", null, true, "A. Jones", "2020-01-01", "Parus major"),
                RowFactory.create(
                    "IDPK-2", "OPK-002", null, true, "B. Lee", "2021-01-01", "Quercus robur")));

    Optional<Dataset<Row>> resultOpt =
        IdentificationExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "occurrence", occ, IdentificationExtensionBuilder.TABLE_IDENTIFICATION, identDf));

    assertTrue(resultOpt.isPresent());
    List<Row> rows = resultOpt.get().collectAsList();
    assertEquals(2, rows.size());
  }

  // ---- helper ----

  @SuppressWarnings("unchecked")
  private List<Map<String, String>> parseHistoryJson(Row row) throws Exception {
    String json = row.getAs(IdentificationExtensionBuilder.COL_IDENTIFICATION_EXT_JSON);
    return MapperUtil.MAPPER.readValue(json, new TypeReference<List<Map<String, String>>>() {});
  }
}
