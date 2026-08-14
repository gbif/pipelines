package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentificationExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IdentificationHistoryMappingParityTest {

  private SparkSession spark;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("IdentificationHistoryMappingParityTest").getOrCreate();
    SchemaGraph graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = OccurrenceDwcaMapping.withIdentificationHistory(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void acceptedAndHistoricalIdentificationRowsBothMatchLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrences(RowFactory.create("OPK-1", "OCC-1")),
            "identification",
            identifications(
                RowFactory.create("IDPK-1", "OPK-1", true, "Accepted name", "Fagaceae"),
                RowFactory.create("IDPK-2", "OPK-1", false, "Historic name", "Betulaceae")));

    List<Map<String, String>> legacy = normalizedLegacy(loader);
    List<Map<String, String>> mapped = mapped(loader);

    String scientificName = TermResolver.resolve("scientificName");
    Comparator<Map<String, String>> byName =
        Comparator.comparing(row -> row.get(scientificName), Comparator.nullsFirst(String::compareTo));
    legacy = legacy.stream().sorted(byName).toList();
    mapped = mapped.stream().sorted(byName).toList();

    assertEquals(2, legacy.size());
    assertEquals(legacy, mapped);

    String accepted = TermResolver.resolve("isAcceptedIdentification");
    assertEquals("true", mapped.get(0).get(accepted));
    assertEquals("false", mapped.get(1).get(accepted));
  }

  @Test
  void identificationRowsStayAttachedToTheirOwnOccurrence() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrences(
                RowFactory.create("OPK-1", "OCC-1"),
                RowFactory.create("OPK-2", "OCC-2")),
            "identification",
            identifications(
                RowFactory.create("IDPK-1", "OPK-1", true, "Oak", "Fagaceae"),
                RowFactory.create("IDPK-2", "OPK-2", true, "Pine", "Pinaceae")));

    List<ExtendedRecord> mapped = engine.execute(loader, plan).collectAsList();
    assertEquals(2, mapped.size());
    Map<String, String> nameByOccurrence =
        mapped.stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    ExtendedRecord::getId,
                    record ->
                        record
                            .getExtensions()
                            .get(IdentificationMapping.ROW_TYPE_IDENTIFICATION)
                            .get(0)
                            .get(TermResolver.resolve("scientificName"))));
    assertEquals("Oak", nameByOccurrence.get("OCC-1"));
    assertEquals("Pine", nameByOccurrence.get("OCC-2"));
  }

  @Test
  void absentIdentificationTableMeansAbsentExtension() {
    TableLoader loader =
        TestTableLoader.of("occurrence", occurrences(RowFactory.create("OPK-1", "OCC-1")));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertFalse(legacy.getExtensions().containsKey(IdentificationExtensionBuilder.ROW_TYPE_IDENTIFICATION));
    assertFalse(mapped.getExtensions().containsKey(IdentificationMapping.ROW_TYPE_IDENTIFICATION));
  }

  @Test
  void traceShowsIdentificationHistoryOwnership() {
    String trace = engine.trace(plan);
    assertTrue(trace.contains("Extension: " + IdentificationMapping.ROW_TYPE_IDENTIFICATION));
    assertTrue(trace.contains("Fragment: occurrence-identification-history"));
    assertTrue(trace.contains("Scope key: identification.occurrence_fk"));
  }

  private List<Map<String, String>> normalizedLegacy(TableLoader loader) {
    ExtendedRecord record = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    List<Map<String, String>> rows =
        record.getExtensions().get(IdentificationExtensionBuilder.ROW_TYPE_IDENTIFICATION);
    assertNotNull(rows);
    String occurrenceId = TermResolver.resolve("occurrenceID");
    return rows.stream()
        .map(
            row -> {
              Map<String, String> copy = new LinkedHashMap<>(row);
              copy.remove(occurrenceId);
              return copy;
            })
        .toList();
  }

  private List<Map<String, String>> mapped(TableLoader loader) {
    ExtendedRecord record = only(engine.execute(loader, plan).collectAsList());
    List<Map<String, String>> rows =
        record.getExtensions().get(IdentificationMapping.ROW_TYPE_IDENTIFICATION);
    assertNotNull(rows);
    return rows;
  }

  private Dataset<Row> occurrences(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> identifications(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("identification_pk", DataTypes.StringType)
            .add("occurrence_fk", DataTypes.StringType)
            .add("isAcceptedIdentification", DataTypes.BooleanType)
            .add("scientificName", DataTypes.StringType)
            .add("family", DataTypes.StringType));
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }
}
