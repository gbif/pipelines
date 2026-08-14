package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import java.util.Comparator;
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
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledCoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventCoreEnrichmentMappingParityTest {

  private SparkSession spark;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("EventCoreEnrichmentMappingParityTest").getOrCreate();
    SchemaGraph graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = EventDwcaMapping.withCoreEnrichments(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void parentEventIsResolvedToNaturalParentEventId() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events(
                RowFactory.create("EPK-P", "EVT-PARENT", null, null, null, null, null, null),
                RowFactory.create("EPK-C", "EVT-CHILD", "EPK-P", null, null, null, null, null)));

    ExtendedRecord legacy = byId(EventCoreBuilder.build(spark, loader).collectAsList(), "EVT-CHILD");
    ExtendedRecord mapped = byId(engine.execute(loader, plan).collectAsList(), "EVT-CHILD");

    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.parentEventID.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.parentEventID.qualifiedName()));
    assertEquals("EVT-PARENT", mapped.getCoreTerms().get(DwcTerm.parentEventID.qualifiedName()));
  }

  @Test
  void geologicalContextFieldsAreFlattenedOntoEvent() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events(RowFactory.create("EPK-1", "EVT001", null, "GEO-1", null, null, null, null)),
            "geological-context",
            geologicalContext());

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.formation.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.formation.qualifiedName()));
    assertEquals("Formation X", mapped.getCoreTerms().get(DwcTerm.formation.qualifiedName()));
  }

  @Test
  void agentIdsResolveNamesAndKeepIds() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events(
                RowFactory.create(
                    "EPK-1", "EVT001", null, null, "AGT-1", "AGT-2", null, null)),
            "agent",
            agents());

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.recordedBy.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.recordedBy.qualifiedName()));
    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.georeferencedBy.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.georeferencedBy.qualifiedName()));
    assertEquals("Jane Doe", mapped.getCoreTerms().get(DwcTerm.recordedBy.qualifiedName()));
    assertEquals("John Roe", mapped.getCoreTerms().get(DwcTerm.georeferencedBy.qualifiedName()));
    assertEquals("AGT-1", mapped.getCoreTerms().get(DwcTerm.recordedByID.qualifiedName()));
  }

  @Test
  void publisherAgentNameWinsOverResolvedAgentName() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events(
                RowFactory.create(
                    "EPK-1",
                    "EVT001",
                    null,
                    null,
                    "AGT-1",
                    null,
                    "Publisher Name",
                    null)),
            "agent",
            agents());

    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());
    assertEquals("Publisher Name", mapped.getCoreTerms().get(DwcTerm.recordedBy.qualifiedName()));
  }

  @Test
  void directProtocolFkResolvesAndFallsBackLikeLegacy() {
    StructType eventSchema =
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("eventProtocol_fk", DataTypes.StringType)
            .add("georeferenceProtocol_fk", DataTypes.StringType)
            .add("georeferenceProtocol", DataTypes.StringType);
    Dataset<Row> event =
        spark.createDataFrame(
            List.of(RowFactory.create("EPK-1", "EVT001", "PPK-1", "PPK-2", "Publisher geo")),
            eventSchema);
    Dataset<Row> protocol =
        protocols(
            RowFactory.create("PPK-1", "sampling", null, "Direct sampling"),
            RowFactory.create("PPK-2", "georeferencing", null, "Resolved geo"));
    TableLoader loader = TestTableLoader.of("event", event, "protocol", protocol);

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()));
    assertEquals("Direct sampling", mapped.getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()));
    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.georeferenceProtocol.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.georeferenceProtocol.qualifiedName()));
    assertEquals("Publisher geo", mapped.getCoreTerms().get(DwcTerm.georeferenceProtocol.qualifiedName()));
  }

  @Test
  void protocolFkRawValuesAreFallbackWhenProtocolTableIsAbsent() {
    StructType eventSchema =
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("eventProtocol_fk", DataTypes.StringType)
            .add("georeferenceProtocol_fk", DataTypes.StringType);
    Dataset<Row> event =
        spark.createDataFrame(
            List.of(RowFactory.create("EPK-1", "EVT001", "PROTO-1", "PROTO-2")), eventSchema);
    TableLoader loader = TestTableLoader.of("event", event);

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()));
    assertEquals("PROTO-1", mapped.getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()));
    assertEquals("PROTO-2", mapped.getCoreTerms().get(DwcTerm.georeferenceProtocol.qualifiedName()));
  }

  @Test
  void directGeoreferenceProtocolUsesTypedName() {
    StructType eventSchema =
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("georeferenceProtocol_fk", DataTypes.StringType);
    Dataset<Row> event =
        spark.createDataFrame(
            List.of(RowFactory.create("EPK-1", "EVT001", "PPK-1")), eventSchema);
    Dataset<Row> protocol =
        protocols(RowFactory.create("PPK-1", "georeferencing", "Hand-held GPS receiver", null));
    TableLoader loader = TestTableLoader.of("event", event, "protocol", protocol);

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.georeferenceProtocol.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.georeferenceProtocol.qualifiedName()));
    assertEquals(
        "georeferencing: Hand-held GPS receiver",
        mapped.getCoreTerms().get(DwcTerm.georeferenceProtocol.qualifiedName()));
  }

  @Test
  void eventAndSurveyProtocolJunctionsMergeDeterministically() {
    Dataset<Row> event =
        spark.createDataFrame(
            List.of(RowFactory.create("EPK-1", "EVT001")),
            new StructType()
                .add("event_pk", DataTypes.StringType)
                .add("eventID", DataTypes.StringType));
    Dataset<Row> eventProtocol =
        spark.createDataFrame(
            List.of(
                RowFactory.create("EPK-1", "PPK-2"),
                RowFactory.create("EPK-1", "PPK-1"),
                RowFactory.create("EPK-1", "PPK-1")),
            new StructType()
                .add("event_fk", DataTypes.StringType)
                .add("protocol_fk", DataTypes.StringType));
    Dataset<Row> survey =
        spark.createDataFrame(
            List.of(RowFactory.create("SPK-1", "EPK-1")),
            new StructType()
                .add("survey_pk", DataTypes.StringType)
                .add("event_fk", DataTypes.StringType));
    Dataset<Row> surveyProtocol =
        spark.createDataFrame(
            List.of(RowFactory.create("SPK-1", "PPK-3")),
            new StructType()
                .add("survey_fk", DataTypes.StringType)
                .add("protocol_fk", DataTypes.StringType));
    Dataset<Row> protocol =
        protocols(
            RowFactory.create("PPK-1", "sampling", "Vegetation plot", null),
            RowFactory.create("PPK-2", "georeferencing", "Hand-held GPS", null),
            RowFactory.create("PPK-3", "samplingEffort", "Until no new species", null));

    TableLoader loader =
        TestTableLoader.of(
            "event", event,
            "event-protocol", eventProtocol,
            "survey", survey,
            "survey-protocol", surveyProtocol,
            "protocol", protocol);

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertPipeSetEquals(
        legacy.getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()));
    assertEquals(
        "georeferencing: Hand-held GPS",
        mapped.getCoreTerms().get(DwcTerm.georeferenceProtocol.qualifiedName()));
  }

  @Test
  void compilerRecordsProtocolTargetsAsExplicitMerges() {
    CompiledMapping compiled = engine.compile(plan);
    assertEquals(2, compiled.coreTargetMerges().size());
    assertTrue(
        compiled.coreDecisions().stream()
            .anyMatch(
                decision ->
                    decision.type()
                            == org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingDecisionType.EXPLICIT_MERGE
                        && decision.targetTerm().equals(DwcTerm.samplingProtocol.qualifiedName())));
  }

  @Test
  void compiledCoreFragmentsRetainStructuredLineage() {
    CompiledMapping compiled = engine.compile(plan);
    assertEquals(10, compiled.coreFragments().size());

    CompiledCoreFragment parent =
        compiled.coreFragments().stream()
            .filter(fragment -> fragment.name().equals("event-parent"))
            .findFirst()
            .orElseThrow();
    assertEquals(1, parent.relations().size());
    assertEquals("parentEvent_fk", parent.relations().get(0).relation().sourceColumn());
    assertTrue(
        parent.targets().stream()
            .anyMatch(target -> target.targetTerm().equals(DwcTerm.parentEventID.qualifiedName())));

    CompiledCoreFragment geology =
        compiled.coreFragments().stream()
            .filter(fragment -> fragment.name().equals("event-geological-context"))
            .findFirst()
            .orElseThrow();
    assertTrue(geology.relations().get(0).explicitColumns());
    assertEquals("geologicalContextID", geology.relations().get(0).relation().sourceColumn());
  }

  private Dataset<Row> events(Row... rows) {
    StructType schema =
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("parentEvent_fk", DataTypes.StringType)
            .add("geologicalContextID", DataTypes.StringType)
            .add("eventConductedByID", DataTypes.StringType)
            .add("georeferencedByID", DataTypes.StringType)
            .add("eventConductedBy", DataTypes.StringType)
            .add("georeferencedBy", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> geologicalContext() {
    StructType schema =
        new StructType()
            .add("geologicalContext_pk", DataTypes.StringType)
            .add("geologicalContextID", DataTypes.StringType)
            .add("earliestEonOrLowestEonothem", DataTypes.StringType)
            .add("formation", DataTypes.StringType);
    return spark.createDataFrame(
        List.of(RowFactory.create("GCPK-1", "GEO-1", "Phanerozoic", "Formation X")), schema);
  }

  private Dataset<Row> agents() {
    StructType schema =
        new StructType()
            .add("agent_pk", DataTypes.StringType)
            .add("agentID", DataTypes.StringType)
            .add("preferredAgentName", DataTypes.StringType);
    return spark.createDataFrame(
        List.of(
            RowFactory.create("APK-1", "AGT-1", "Jane Doe"),
            RowFactory.create("APK-2", "AGT-2", "John Roe")),
        schema);
  }

  private Dataset<Row> protocols(Row... rows) {
    StructType schema =
        new StructType()
            .add("protocol_pk", DataTypes.StringType)
            .add("protocolType", DataTypes.StringType)
            .add("protocolName", DataTypes.StringType)
            .add("protocolDescription", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private static void assertPipeSetEquals(String expected, String actual) {
    assertNotNull(expected);
    assertNotNull(actual);
    assertEquals(
        Arrays.stream(expected.split("\\|"))
            .filter(value -> !value.isBlank())
            .collect(java.util.stream.Collectors.toSet()),
        Arrays.stream(actual.split("\\|"))
            .filter(value -> !value.isBlank())
            .collect(java.util.stream.Collectors.toSet()));
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }

  private static ExtendedRecord byId(List<ExtendedRecord> records, String id) {
    return records.stream().filter(record -> id.equals(record.getId())).findFirst().orElseThrow();
  }
}
