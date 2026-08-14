package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.builder.extension.AssertionExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledTargetProducer;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AssertionMappingParityTest {

  private SparkSession spark;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("AssertionMappingParityTest").getOrCreate();
    SchemaGraph graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = EventDwcaMapping.withAssertions(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void directEventAssertionMatchesLegacyPayload() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            event(),
            "event-assertion",
            assertions(
                RowFactory.create(
                    "A001", "EPK-001", "Temperature", null, "25.0", null, "C", null,
                    "0.1", "Alice", "2026-08-14", "Calibrated", null)));

    assertPayloadParity(loader);

    Map<String, String> row = mappedAssertions(loader).get(0);
    assertEquals("A001", row.get(DwcTerm.measurementID.qualifiedName()));
    assertEquals("Temperature", row.get(DwcTerm.measurementType.qualifiedName()));
    assertEquals("25.0", row.get(DwcTerm.measurementValue.qualifiedName()));
    assertEquals("C", row.get(DwcTerm.measurementUnit.qualifiedName()));
  }

  @Test
  void protocolDescriptionBecomesMeasurementMethod() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            event(),
            "event-assertion",
            assertions(
                RowFactory.create(
                    "A001", "EPK-001", "Temperature", null, "25.0", null, "C", null,
                    null, null, null, null, "PPK-1")),
            "protocol",
            protocol("PPK-1", "Thermometer calibration protocol"));

    assertPayloadParity(loader);
    assertEquals(
        "Thermometer calibration protocol",
        mappedAssertions(loader).get(0).get(DwcTerm.measurementMethod.qualifiedName()));
  }

  @Test
  void absentProtocolTableKeepsRawProtocolFkFallback() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            event(),
            "event-assertion",
            assertions(
                RowFactory.create(
                    "A001", "EPK-001", "Temperature", null, "25.0", null, "C", null,
                    null, null, null, null, "PPK-RAW")));

    assertPayloadParity(loader);
    assertEquals(
        "PPK-RAW",
        mappedAssertions(loader).get(0).get(DwcTerm.measurementMethod.qualifiedName()));
  }

  @Test
  void multipleAssertionsRemainSeparateRows() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            event(),
            "event-assertion",
            assertions(
                RowFactory.create(
                    "A001", "EPK-001", "Temperature", null, "25.0", null, "C", null,
                    null, null, null, null, null),
                RowFactory.create(
                    "A002", "EPK-001", "Humidity", null, "80", null, "%", null,
                    null, null, null, null, null)));

    assertPayloadParity(loader);
    assertEquals(2, mappedAssertions(loader).size());
  }

  @Test
  void danglingEventAssertionDoesNotAttach() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            event(),
            "event-assertion",
            assertions(
                RowFactory.create(
                    "A001", "EPK-DANGLING", "Temperature", null, "25.0", null, "C", null,
                    null, null, null, null, null)));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertFalse(
        legacy
            .getExtensions()
            .containsKey(AssertionExtensionBuilder.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT));
    assertFalse(
        mapped
            .getExtensions()
            .containsKey(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT));
  }

  @Test
  void compiledMappingRetainsExplicitAssertionRemapsAndProtocolLookup() {
    CompiledMapping compiled = engine.compile(plan);
    CompiledExtension extension =
        compiled.extensions().stream()
            .filter(
                e ->
                    e.rowType()
                        .equals(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT))
            .findFirst()
            .orElseThrow();
    CompiledFragment fragment =
        extension.fragments().stream()
            .filter(f -> f.name().equals("event-assertions"))
            .findFirst()
            .orElseThrow();

    SchemaPath assertion = SchemaPath.root("event-assertion");
    assertEquals(assertion.field("event_fk"), fragment.scopeKey());
    assertEquals(assertion.field("assertionID"), fragment.rowIdentity().orElseThrow());

    CompiledTargetProducer measurementType =
        producer(fragment, DwcTerm.measurementType.qualifiedName());
    assertEquals(List.of(assertion.field("assertionType")), sourceFields(measurementType));

    CompiledTargetProducer measurementMethod =
        producer(fragment, DwcTerm.measurementMethod.qualifiedName());
    assertEquals(2, measurementMethod.sources().size());
    assertEquals("protocolDescription", measurementMethod.sources().get(0).field().column());
    assertEquals(assertion.field("assertionProtocol_fk"), measurementMethod.sources().get(1).field());
  }

  private static CompiledTargetProducer producer(CompiledFragment fragment, String targetTerm) {
    return fragment.targets().stream()
        .filter(target -> target.targetTerm().equals(targetTerm))
        .findFirst()
        .orElseThrow();
  }

  private static List<FieldRef> sourceFields(CompiledTargetProducer producer) {
    return producer.sources().stream().map(source -> source.field()).toList();
  }

  private void assertPayloadParity(TableLoader loader) {
    List<Map<String, String>> legacy = normalizeLegacyAssertions(legacyAssertions(loader));
    List<Map<String, String>> mapped = mappedAssertions(loader);
    String measurementId = DwcTerm.measurementID.qualifiedName();
    assertEquals(
        legacy.stream().sorted(java.util.Comparator.comparing(r -> r.get(measurementId))).toList(),
        mapped.stream().sorted(java.util.Comparator.comparing(r -> r.get(measurementId))).toList());
  }

  private List<Map<String, String>> legacyAssertions(TableLoader loader) {
    ExtendedRecord record = only(EventCoreBuilder.build(spark, loader).collectAsList());
    List<Map<String, String>> rows =
        record
            .getExtensions()
            .get(AssertionExtensionBuilder.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> mappedAssertions(TableLoader loader) {
    ExtendedRecord record = only(engine.execute(loader, plan).collectAsList());
    List<Map<String, String>> rows =
        record.getExtensions().get(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> normalizeLegacyAssertions(List<Map<String, String>> rows) {
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

  private Dataset<Row> event() {
    return spark.createDataFrame(
        List.of(RowFactory.create("EPK-001", "EVT001")),
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType));
  }

  private Dataset<Row> assertions(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("assertionID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("assertionType", DataTypes.StringType)
            .add("assertionTypeIRI", DataTypes.StringType)
            .add("assertionValue", DataTypes.StringType)
            .add("assertionValueIRI", DataTypes.StringType)
            .add("assertionUnit", DataTypes.StringType)
            .add("assertionUnitIRI", DataTypes.StringType)
            .add("assertionError", DataTypes.StringType)
            .add("assertionBy", DataTypes.StringType)
            .add("assertionMadeDate", DataTypes.StringType)
            .add("assertionRemarks", DataTypes.StringType)
            .add("assertionProtocol_fk", DataTypes.StringType));
  }

  private Dataset<Row> protocol(String pk, String description) {
    return spark.createDataFrame(
        List.of(RowFactory.create(pk, description)),
        new StructType()
            .add("protocol_pk", DataTypes.StringType)
            .add("protocolDescription", DataTypes.StringType));
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }
}
