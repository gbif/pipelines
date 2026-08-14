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
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentifierExtensionBuilder;
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
class IdentifierMappingParityTest {

  private SparkSession spark;
  private SchemaGraph graph;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("IdentifierMappingParityTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = EventDwcaMapping.withIdentifier(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void directEventIdentifiersMatchLegacyBuilder() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            event("EPK-001", "EVT001"),
            IdentifierExtensionBuilder.TABLE_EVENT_IDENTIFIER,
            eventIdentifiers(
                RowFactory.create("EPK-001", "https://example.org/event/1", "URI"),
                RowFactory.create("EPK-001", "urn:event:1", "internal")));

    List<Map<String, String>> legacy = normalizeLegacyIdentifiers(legacyIdentifiers(loader));
    List<Map<String, String>> mapped = mappedIdentifiers(loader);

    assertEquals(2, legacy.size());
    assertEquals(legacy.size(), mapped.size());

    String identifier = TermResolver.resolve("identifier");
    Comparator<Map<String, String>> byIdentifier =
        Comparator.comparing(row -> row.get(identifier), Comparator.nullsFirst(String::compareTo));
    legacy = legacy.stream().sorted(byIdentifier).toList();
    mapped = mapped.stream().sorted(byIdentifier).toList();

    assertEquals(legacy, mapped);
  }

  @Test
  void duplicatePhysicalIdentifierRowsRemainDistinctLikeLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            event("EPK-001", "EVT001"),
            IdentifierExtensionBuilder.TABLE_EVENT_IDENTIFIER,
            eventIdentifiers(
                RowFactory.create("EPK-001", "urn:event:1", "internal"),
                RowFactory.create("EPK-001", "urn:event:1", "internal")));

    List<Map<String, String>> legacy = normalizeLegacyIdentifiers(legacyIdentifiers(loader));
    List<Map<String, String>> mapped = mappedIdentifiers(loader);

    assertEquals(2, legacy.size());
    assertEquals(legacy, mapped);
  }

  @Test
  void absentEventIdentifierTableMeansAbsentExtension() {
    TableLoader loader = TestTableLoader.of("event", event("EPK-001", "EVT001"));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertFalse(legacy.getExtensions().containsKey(IdentifierExtensionBuilder.ROW_TYPE_IDENTIFIER));
    assertFalse(mapped.getExtensions().containsKey(IdentifierMapping.ROW_TYPE_IDENTIFIER));
  }

  @Test
  void danglingEventIdentifierDoesNotCreateExtensionRow() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            event("EPK-001", "EVT001"),
            IdentifierExtensionBuilder.TABLE_EVENT_IDENTIFIER,
            eventIdentifiers(
                RowFactory.create("EPK-DANGLING", "https://example.org/event/x", "URI")));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertFalse(legacy.getExtensions().containsKey(IdentifierExtensionBuilder.ROW_TYPE_IDENTIFIER));
    assertFalse(mapped.getExtensions().containsKey(IdentifierMapping.ROW_TYPE_IDENTIFIER));
  }

  @Test
  void identifiersStayAttachedToTheirOwnEvents() {
    Dataset<Row> events =
        spark.createDataFrame(
            List.of(
                RowFactory.create("EPK-001", "EVT001"),
                RowFactory.create("EPK-002", "EVT002")),
            new StructType()
                .add("event_pk", DataTypes.StringType)
                .add("eventID", DataTypes.StringType));
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events,
            IdentifierExtensionBuilder.TABLE_EVENT_IDENTIFIER,
            eventIdentifiers(
                RowFactory.create("EPK-001", "urn:event:1", "internal"),
                RowFactory.create("EPK-002", "urn:event:2", "internal")));

    List<ExtendedRecord> legacy = EventCoreBuilder.build(spark, loader).collectAsList();
    List<ExtendedRecord> mapped = engine.execute(loader, plan).collectAsList();

    assertEquals(2, legacy.size());
    assertEquals(2, mapped.size());
    assertEquals(identifierByRecordId(legacy), identifierByRecordId(mapped));
  }

  @Test
  void governingTraceContainsIdentifierSourceAndTargets() {
    String trace = engine.trace(plan);

    assertTrue(trace.contains("Extension: " + IdentifierMapping.ROW_TYPE_IDENTIFIER));
    assertTrue(trace.contains("Fragment: event-identifiers"));
    assertTrue(trace.contains("Source: event-identifier"));
    assertTrue(trace.contains("Scope key: event-identifier.event_fk"));
    assertTrue(trace.contains("event-identifier.identifier"));
    assertTrue(trace.contains(TermResolver.resolve("identifier")));
  }

  private List<Map<String, String>> legacyIdentifiers(TableLoader loader) {
    ExtendedRecord record = only(EventCoreBuilder.build(spark, loader).collectAsList());
    List<Map<String, String>> rows =
        record.getExtensions().get(IdentifierExtensionBuilder.ROW_TYPE_IDENTIFIER);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> mappedIdentifiers(TableLoader loader) {
    ExtendedRecord record = only(engine.execute(loader, plan).collectAsList());
    List<Map<String, String>> rows = record.getExtensions().get(IdentifierMapping.ROW_TYPE_IDENTIFIER);
    assertNotNull(rows);
    return rows;
  }

  /**
   * Legacy ExtensionAggregator serializes its grouping key (eventID) into every extension payload.
   * The mapping engine keeps attachment identity separate from serialized extension terms, so remove
   * that legacy transport detail before comparing semantic Identifier payloads.
   */
  private List<Map<String, String>> normalizeLegacyIdentifiers(List<Map<String, String>> rows) {
    String eventId = TermResolver.resolve("eventID");
    return rows.stream()
        .map(
            row -> {
              Map<String, String> normalized = new LinkedHashMap<>(row);
              normalized.remove(eventId);
              return normalized;
            })
        .toList();
  }

  private Map<String, String> identifierByRecordId(List<ExtendedRecord> records) {
    String identifier = TermResolver.resolve("identifier");
    return records.stream()
        .collect(
            java.util.stream.Collectors.toMap(
                ExtendedRecord::getId,
                record -> record.getExtensions().get(IdentifierMapping.ROW_TYPE_IDENTIFIER).get(0).get(identifier)));
  }

  private Dataset<Row> event(String eventPk, String eventId) {
    return spark.createDataFrame(
        List.of(RowFactory.create(eventPk, eventId)),
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType));
  }

  private Dataset<Row> eventIdentifiers(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType)
            .add("identifierType", DataTypes.StringType));
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }
}
