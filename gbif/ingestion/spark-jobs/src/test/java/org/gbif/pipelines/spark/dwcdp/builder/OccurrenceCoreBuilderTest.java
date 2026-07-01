package org.gbif.pipelines.spark.dwcdp.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.DwcDpRowTypes;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OccurrenceCoreBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("OccurrenceCoreBuilderTest").getOrCreate();
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
            .add("eventID", DataTypes.StringType)
            .add("organismID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> organismDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("organismID", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add("associatedOrganisms", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private static TableLoader loaderOccurrenceOnly(Dataset<Row> occurrenceDf) {
    return tableName ->
        "occurrence".equals(tableName) ? Optional.of(occurrenceDf) : Optional.empty();
  }

  private static TableLoader loaderWithOrganism(Dataset<Row> occurrenceDf, Dataset<Row> orgDf) {
    return tableName ->
        switch (tableName) {
          case "occurrence" -> Optional.of(occurrenceDf);
          case "organism" -> Optional.of(orgDf);
          default -> Optional.empty();
        };
  }

  // ---- tests ----

  @Test
  void missingOccurrenceTable_throws() {
    TableLoader emptyLoader = tableName -> Optional.empty();

    assertThrows(
        IllegalStateException.class,
        () -> OccurrenceCoreBuilder.build(spark, emptyLoader),
        "Should throw when occurrence table is absent — routing error in orchestrator");
  }

  @Test
  void basicOccurrence_producesExtendedRecordWithCorrectCoreRowType() {
    Dataset<Row> occ =
        occurrenceDf(List.of(RowFactory.create("occ-1", "evt-1", null, "Parus major")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderOccurrenceOnly(occ)).collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord er = records.get(0);
    assertEquals("occ-1", er.getId());
    assertEquals(DwcDpRowTypes.CORE_ROW_TYPE_OCCURRENCE, er.getCoreRowType());
    assertTrue(er.getExtensions().isEmpty());
  }

  @Test
  void occurrenceWithNullId_isFiltered() {
    Dataset<Row> occ =
        occurrenceDf(
            List.of(
                RowFactory.create(null, "evt-1", null, "Parus major"),
                RowFactory.create("occ-2", "evt-1", null, "Parus minor")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderOccurrenceOnly(occ)).collectAsList();

    assertEquals(1, records.size());
    assertEquals("occ-2", records.get(0).getId());
  }

  @Test
  void organismFieldsDenormalizedOntoCoreTerms() {
    Dataset<Row> occ =
        occurrenceDf(List.of(RowFactory.create("occ-1", "evt-1", "org-1", "Parus major")));
    Dataset<Row> orgDf =
        organismDf(List.of(RowFactory.create("org-1", "Great tit", "sibling of:org-2")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

    assertEquals(1, records.size());
    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    assertEquals(
        "sibling of:org-2",
        coreTerms.get(DwcTerm.associatedOrganisms.qualifiedName()),
        "associatedOrganisms from organism table must appear on the occurrence core row");
  }

  @Test
  void multipleOccurrencesSameOrganism_eachGetsOrganismFields() {
    Dataset<Row> occ =
        occurrenceDf(
            List.of(
                RowFactory.create("occ-1", "evt-1", "org-1", "Parus major"),
                RowFactory.create("occ-2", "evt-2", "org-1", "Parus major")));
    Dataset<Row> orgDf = organismDf(List.of(RowFactory.create("org-1", "Great tit", null)));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

    assertEquals(2, records.size());
    for (ExtendedRecord er : records) {
      assertEquals(
          "Great tit",
          er.getCoreTerms().get(DwcTerm.organismName.qualifiedName()),
          "Each occurrence should carry the organism name — many:1 collapse");
    }
  }
}
