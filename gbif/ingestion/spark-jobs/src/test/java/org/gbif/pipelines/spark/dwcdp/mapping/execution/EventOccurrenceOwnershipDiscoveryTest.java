package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
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
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventOccurrenceNestedContextMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.config.OccurrenceMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(30)
class EventOccurrenceOwnershipDiscoveryTest {
  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName(getClass().getSimpleName()).getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void discoversOccurrenceThroughDirectMaterialIdentificationAndEnrichesMaterial() {
    ExtendedRecord record =
        execute(
            occurrences(row("O1", "occ-1", null)),
            materials(row("M1", "E1", null, null, "CAT-1", null)),
            identifications(row("I1", "M1", null, null, "O1")),
            null,
            null);

    Map<String, String> occurrence = onlyOccurrence(record);
    assertEquals("occ-1", occurrence.get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals("CAT-1", occurrence.get(DwcTerm.catalogNumber.qualifiedName()));
  }

  @Test
  void discoversOccurrenceThroughMaterialAnalysisIdentification() {
    ExtendedRecord record =
        execute(
            occurrences(row("O1", "occ-1", null)),
            materials(row("M1", "E1", null, null, "CAT-1", null)),
            identifications(row("I1", null, "A1", null, "O1")),
            analyses(row("A1", "M1", null)),
            null);

    assertEquals("CAT-1", onlyOccurrence(record).get(DwcTerm.catalogNumber.qualifiedName()));
  }

  @Test
  void discoversOccurrenceThroughMaterialAnalysisSequenceIdentification() {
    ExtendedRecord record =
        execute(
            occurrences(row("O1", "occ-1", null)),
            materials(row("M1", "E1", null, null, "CAT-1", null)),
            identifications(row("I1", null, null, "S1", "O1")),
            analyses(row("A1", "M1", "S1")),
            sequences("S1"));

    assertEquals("CAT-1", onlyOccurrence(record).get(DwcTerm.catalogNumber.qualifiedName()));
  }

  @Test
  void oneMaterialMayFanOutToMultipleOccurrences() {
    ExtendedRecord record =
        execute(
            occurrences(row("O1", "occ-1", null), row("O2", "occ-2", null)),
            materials(row("M1", "E1", null, null, "CAT-1", null)),
            identifications(row("I1", "M1", null, null, "O1"), row("I2", "M1", null, null, "O2")),
            null,
            null);

    List<Map<String, String>> occurrences = occurrences(record);
    assertEquals(2, occurrences.size());
    assertEquals(
        List.of("occ-1", "occ-2"),
        occurrences.stream()
            .map(row -> row.get(DwcTerm.occurrenceID.qualifiedName()))
            .sorted()
            .toList());
  }

  @Test
  void multipleDiscoveryPathsToSameOccurrenceProduceOneRow() {
    ExtendedRecord record =
        execute(
            occurrences(row("O1", "occ-1", null)),
            materials(row("M1", "E1", null, null, "CAT-1", null)),
            identifications(row("I1", "M1", null, null, "O1"), row("I2", null, "A1", null, "O1")),
            analyses(row("A1", "M1", null)),
            null);

    assertEquals(1, occurrences(record).size());
  }

  @Test
  void distinctMaterialsForSameEventOccurrenceSuppressMaterialEnrichment() {
    ExtendedRecord record =
        execute(
            occurrences(row("O1", "occ-1", null)),
            materials(
                row("M1", "E1", null, null, "CAT-1", null),
                row("M2", "E1", null, null, "CAT-2", null)),
            identifications(row("I1", "M1", null, null, "O1"), row("I2", "M2", null, null, "O1")),
            null,
            null);

    Map<String, String> occurrence = onlyOccurrence(record);
    assertFalse(occurrence.containsKey(DwcTerm.catalogNumber.qualifiedName()));
  }

  @Test
  void evidenceAndDifferentDiscoveredMaterialAreTreatedAsAmbiguousTogether() {
    ExtendedRecord record =
        execute(
            occurrences(row("O1", "occ-1", "E1")),
            materials(
                row("M1", "E1", "occ-1", null, "CAT-EVIDENCE", null),
                row("M2", "E1", null, null, "CAT-DISCOVERED", null)),
            identifications(row("I1", "M2", null, null, "O1")),
            null,
            null);

    assertFalse(onlyOccurrence(record).containsKey(DwcTerm.catalogNumber.qualifiedName()));
  }

  @Test
  void materialBasisOfRecordPrecedesOwningEventTypeForDiscoveredOccurrence() {
    ExtendedRecord record =
        execute(
            "Sensor",
            occurrences(row("O1", "occ-1", null)),
            materials(row("M1", "E1", null, null, "CAT-1", "preserved")),
            identifications(row("I1", "M1", null, null, "O1")),
            null,
            null);

    assertEquals(
        "PreservedSpecimen",
        onlyOccurrence(record).get(DwcTerm.basisOfRecord.qualifiedName()));
  }

  @Test
  void owningEventBasisOfRecordAppliesAfterMaterialFallsThrough() {
    ExtendedRecord record =
        execute(
            "Sensor",
            occurrences(row("O1", "occ-1", null)),
            materials(row("M1", "E1", null, null, "CAT-1", "other")),
            identifications(row("I1", "M1", null, null, "O1")),
            null,
            null);

    assertEquals(
        "MachineObservation",
        onlyOccurrence(record).get(DwcTerm.basisOfRecord.qualifiedName()));
  }

  @Test
  void directOccurrenceUsesOwningEventBasisOfRecordWithoutMaterialTable() {
    ExtendedRecord record =
        execute(
            "Observation",
            occurrences(row("O1", "occ-1", "E1")),
            null,
            null,
            null,
            null);

    assertEquals(
        "HumanObservation",
        onlyOccurrence(record).get(DwcTerm.basisOfRecord.qualifiedName()));
  }

  @Test
  void basisOfRecordFallsBackToOccurrence() {
    ExtendedRecord record =
        execute(
            "Sampling",
            occurrences(row("O1", "occ-1", "E1")),
            null,
            null,
            null,
            null);

    assertEquals("Occurrence", onlyOccurrence(record).get(DwcTerm.basisOfRecord.qualifiedName()));
  }

  private ExtendedRecord execute(
      Dataset<Row> occurrence,
      Dataset<Row> material,
      Dataset<Row> identification,
      Dataset<Row> analysis,
      Dataset<Row> sequence) {
    return execute("Sensor", occurrence, material, identification, analysis, sequence);
  }

  private ExtendedRecord execute(
      String eventType,
      Dataset<Row> occurrence,
      Dataset<Row> material,
      Dataset<Row> identification,
      Dataset<Row> analysis,
      Dataset<Row> sequence) {
    Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
    tables.put("event", events(eventType));
    tables.put("occurrence", occurrence);
    if (material != null) {
      tables.put("material", material);
    }
    if (identification != null) {
      tables.put("identification", identification);
    }
    if (analysis != null) {
      tables.put("nucleotide-analysis", analysis);
    }
    if (sequence != null) {
      tables.put("nucleotide-sequence", sequence);
    }
    TableLoader loader = resource -> Optional.ofNullable(tables.get(resource));

    ExtendedRecord record =
        new SparkExtendedRecordExecutor(graph).execute(loader, plan()).collectAsList().get(0);
    assertEquals("EV1", record.getId());
    return record;
  }

  private MappingPlan plan() {
    SchemaPath event = SchemaPath.root("event");
    return MappingPlanBuilder.mappingPlan("event-occurrence-ownership", CoreType.EVENT, "event")
        .nestedExtensionContext(EventOccurrenceNestedContextMapping.context(graph))
        .coreIdentity(
            ValueAggregation.firstOrUrnFallback("urn:gbif:dwcdp:event:"),
            event.field("eventID"),
            event.field("event_pk"))
        .coreField(
            TargetFieldMapping.oneOf(
                DwcTerm.eventID.qualifiedName(),
                ValueAggregation.firstNonNull(),
                event.field("eventID")))
        .extension(OccurrenceMapping.ROW_TYPE_OCCURRENCE)
        .mergeTarget(DwcTerm.occurrenceID.qualifiedName(), ValueAggregation.firstNonNull())
        .mergeTarget(DwcTerm.basisOfRecord.qualifiedName(), ValueAggregation.firstNonNull())
        .importFragment(OccurrenceMapping.directOccurrence(graph))
        .importFragment(OccurrenceMapping.material(graph))
        .importFragment(OccurrenceMapping.materialBasisOfRecord(graph))
        .importFragment(OccurrenceMapping.eventBasisOfRecord(graph))
        .importFragment(OccurrenceMapping.defaultBasisOfRecord(graph))
        .build();
  }

  private Map<String, String> onlyOccurrence(ExtendedRecord record) {
    List<Map<String, String>> rows = occurrences(record);
    assertEquals(1, rows.size());
    return rows.get(0);
  }

  private List<Map<String, String>> occurrences(ExtendedRecord record) {
    List<Map<String, String>> rows =
        record.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE);
    assertNotNull(rows);
    return rows;
  }

  private Dataset<Row> events(String eventType) {
    return spark.createDataFrame(
        List.of(RowFactory.create("E1", "EV1", eventType)),
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("eventType", DataTypes.StringType));
  }

  private Dataset<Row> occurrences(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType));
  }

  private Dataset<Row> materials(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("collectionEvent_fk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("catalogNumber", DataTypes.StringType)
            .add("materialEntityCategory", DataTypes.StringType));
  }

  private Dataset<Row> identifications(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("identification_pk", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
            .add("nucleotideAnalysis_fk", DataTypes.StringType)
            .add("nucleotideSequence_fk", DataTypes.StringType)
            .add("occurrence_fk", DataTypes.StringType));
  }

  private Dataset<Row> analyses(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("nucleotideAnalysis_pk", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
            .add("nucleotideSequence_fk", DataTypes.StringType));
  }

  private Dataset<Row> sequences(String... ids) {
    return spark.createDataFrame(
        Arrays.stream(ids).map(RowFactory::create).toList(),
        new StructType().add("nucleotideSequence_pk", DataTypes.StringType));
  }

  private static Row row(Object... values) {
    return RowFactory.create(values);
  }
}
