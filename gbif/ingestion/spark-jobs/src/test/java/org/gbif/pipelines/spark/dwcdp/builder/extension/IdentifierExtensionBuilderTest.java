package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IdentifierExtensionBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder().appName("IdentifierExtensionBuilderTest").getOrCreate();
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

  private Dataset<Row> eventPkDf(List<Row> rows) {
    StructType schema =
        new StructType().add("event_pk", DataTypes.StringType).add("eventID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventIdentifierDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType)
            .add("identifierType", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceIdentifierDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrence_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType)
            .add("identifierType", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialIdentifierDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType)
            .add("identifierType", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- absent-table cases ----

  @Test
  void neitherTablePresent_returnsEmpty() {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));

    Optional<Dataset<Row>> result =
        IdentifierExtensionBuilder.build(spark, TestTableLoader.of("occurrence", occ));

    assertTrue(result.isEmpty());
  }

  // ---- event-identifier ----

  @Test
  void directEventIdentifiers_areResolvedAndAggregated() throws Exception {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> identifiers =
        eventIdentifierDf(
            List.of(
                RowFactory.create("EPK-001", "https://example.org/event/1", "URI"),
                RowFactory.create("EPK-001", "urn:event:1", "internal")));

    Optional<Dataset<Row>> result =
        IdentifierExtensionBuilder.buildEvent(
            spark,
            TestTableLoader.of(
                "event", event, IdentifierExtensionBuilder.TABLE_EVENT_IDENTIFIER, identifiers));

    assertTrue(result.isPresent());
    List<Map<String, String>> ext = parseIdentifierJson(result.get().collectAsList().get(0));
    assertEquals(2, ext.size());
  }

  @Test
  void danglingEventIdentifier_isDropped() {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> identifiers =
        eventIdentifierDf(
            List.of(RowFactory.create("EPK-DANGLING", "https://example.org/event/x", "URI")));

    Optional<Dataset<Row>> result =
        IdentifierExtensionBuilder.buildEvent(
            spark,
            TestTableLoader.of(
                "event", event, IdentifierExtensionBuilder.TABLE_EVENT_IDENTIFIER, identifiers));

    assertTrue(result.isEmpty());
  }

  // ---- direct occurrence-identifier only ----

  @Test
  void directOccurrenceIdentifier_producesExtension() throws Exception {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> identifierDf =
        occurrenceIdentifierDf(
            List.of(RowFactory.create("OPK-001", "https://example.org/occ/1", "URI")));

    Optional<Dataset<Row>> resultOpt =
        IdentifierExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "occurrence",
                occ,
                IdentifierExtensionBuilder.TABLE_OCCURRENCE_IDENTIFIER,
                identifierDf));

    assertTrue(resultOpt.isPresent());
    List<Map<String, String>> ext = parseIdentifierJson(resultOpt.get().collectAsList().get(0));
    assertEquals(1, ext.size());
  }

  // ---- material-identifier merge ----

  @Test
  void materialIdentifier_mergedWhenNoDirectIdentifier() throws Exception {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-1", "OCC001")));
    Dataset<Row> materialIdentifierDf =
        materialIdentifierDf(
            List.of(RowFactory.create("MEPK-1", "urn:catalog:NHMD:AVES:12345", "barcode")));

    Optional<Dataset<Row>> resultOpt =
        IdentifierExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "occurrence",
                occ,
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                IdentifierExtensionBuilder.TABLE_MATERIAL_IDENTIFIER,
                materialIdentifierDf));

    assertTrue(
        resultOpt.isPresent(),
        "a specimen barcode must still produce an identifier extension even with no direct "
            + "occurrence-identifier at all");
    List<Map<String, String>> ext = parseIdentifierJson(resultOpt.get().collectAsList().get(0));
    assertEquals(1, ext.size());
  }

  @Test
  void directAndMaterialIdentifier_bothPresent_unionedTogether() throws Exception {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-1", "OCC001")));
    Dataset<Row> occurrenceIdentifierDf =
        occurrenceIdentifierDf(
            List.of(RowFactory.create("OPK-001", "https://example.org/occ/1", "URI")));
    Dataset<Row> materialIdentifierDf =
        materialIdentifierDf(
            List.of(RowFactory.create("MEPK-1", "urn:catalog:NHMD:AVES:12345", "barcode")));

    Optional<Dataset<Row>> resultOpt =
        IdentifierExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "occurrence",
                occ,
                IdentifierExtensionBuilder.TABLE_OCCURRENCE_IDENTIFIER,
                occurrenceIdentifierDf,
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                IdentifierExtensionBuilder.TABLE_MATERIAL_IDENTIFIER,
                materialIdentifierDf));

    assertTrue(resultOpt.isPresent());
    List<Map<String, String>> ext = parseIdentifierJson(resultOpt.get().collectAsList().get(0));
    assertEquals(
        2, ext.size(), "both the occurrence's own and the specimen's identifier must appear");
  }

  @Test
  void materialIdentifier_notMergedWhenOccurrenceHasAmbiguousMaterial() {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> material =
        materialDf(
            List.of(RowFactory.create("MEPK-1", "OCC001"), RowFactory.create("MEPK-2", "OCC001")));
    Dataset<Row> materialIdentifierDf =
        materialIdentifierDf(
            List.of(RowFactory.create("MEPK-1", "urn:catalog:NHMD:AVES:12345", "barcode")));

    Optional<Dataset<Row>> resultOpt =
        IdentifierExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "occurrence",
                occ,
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                IdentifierExtensionBuilder.TABLE_MATERIAL_IDENTIFIER,
                materialIdentifierDf));

    assertTrue(
        resultOpt.isEmpty(),
        "with no direct occurrence-identifier and an ambiguous (two-record) material link, there "
            + "is nothing unambiguous to merge in at all");
  }

  // ---- dangling FK safety (the general fix applied to resolveToParentId) ----

  @Test
  void danglingOccurrenceFk_rowDroppedNotNullKeyed() {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    // References an occurrence_pk that doesn't exist in the occurrence table at all.
    Dataset<Row> identifierDf =
        occurrenceIdentifierDf(
            List.of(RowFactory.create("OPK-DANGLING", "https://example.org/occ/x", "URI")));

    Optional<Dataset<Row>> result =
        IdentifierExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "occurrence",
                occ,
                IdentifierExtensionBuilder.TABLE_OCCURRENCE_IDENTIFIER,
                identifierDf));

    assertTrue(
        result.isEmpty(),
        "a dangling occurrence_fk must be dropped, not survive as a null-keyed aggregated row");
  }

  // ---- helper ----

  @SuppressWarnings("unchecked")
  private List<Map<String, String>> parseIdentifierJson(Row row) throws Exception {
    String json = row.getAs(IdentifierExtensionBuilder.COL_IDENTIFIER_EXT_JSON);
    return MapperUtil.MAPPER.readValue(json, new TypeReference<List<Map<String, String>>>() {});
  }

  @Test
  void eventTableWithNoEventIdColumn_doesNotThrow() {
    // eventID has no `required: true` constraint in the DwC-DP profile — a package that never
    // populated it can legitimately arrive with the column absent entirely.
    StructType schema = new StructType().add("event_pk", DataTypes.StringType);
    Dataset<Row> eventDf = spark.createDataFrame(List.of(RowFactory.create("EPK-001")), schema);
    Dataset<Row> identifierDf =
        eventIdentifierDf(List.of(RowFactory.create("EPK-001", "barcode-001", "barcode")));

    Optional<Dataset<Row>> result =
        assertDoesNotThrow(
            () ->
                IdentifierExtensionBuilder.buildEvent(
                    spark,
                    TestTableLoader.of(
                        "event",
                        eventDf,
                        IdentifierExtensionBuilder.TABLE_EVENT_IDENTIFIER,
                        identifierDf)),
            "a missing eventID column must not crash the event identifier extension");

    assertTrue(result.isEmpty());
  }
}
