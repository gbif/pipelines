package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventOccurrenceNestedContextMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.config.OccurrenceMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.NestedExtensionContext;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression;
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
    assertTrue(
        occurrences.stream()
            .allMatch(row -> "CAT-1".equals(row.get(DwcTerm.catalogNumber.qualifiedName()))));
    assertNoSyntheticValues(record);
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

    List<Map<String, String>> occurrences = occurrences(record);
    assertEquals(1, occurrences.size());
    assertEquals("CAT-1", occurrences.get(0).get(DwcTerm.catalogNumber.qualifiedName()));
    assertNoSyntheticValues(record);
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
        "PreservedSpecimen", onlyOccurrence(record).get(DwcTerm.basisOfRecord.qualifiedName()));
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
        "MachineObservation", onlyOccurrence(record).get(DwcTerm.basisOfRecord.qualifiedName()));
  }

  @Test
  void directOccurrenceUsesOwningEventBasisOfRecordWithoutMaterialTable() {
    ExtendedRecord record =
        execute("Observation", occurrences(row("O1", "occ-1", "E1")), null, null, null, null);

    assertEquals(
        "HumanObservation", onlyOccurrence(record).get(DwcTerm.basisOfRecord.qualifiedName()));
  }

  @Test
  void basisOfRecordFallsBackToOccurrence() {
    ExtendedRecord record =
        execute("Sampling", occurrences(row("O1", "occ-1", "E1")), null, null, null, null);

    assertEquals("Occurrence", onlyOccurrence(record).get(DwcTerm.basisOfRecord.qualifiedName()));
  }

  @Test
  void missingOccurrenceIdUsesPublicPrimaryKeyFallbackAndNeverSyntheticContextId() {
    ExtendedRecord record =
        execute(
            occurrences(row("O1", null, null)),
            materials(row("M1", "E1", null, null, "CAT-1", null)),
            identifications(row("I1", "M1", null, null, "O1")),
            null,
            null);

    Map<String, String> occurrence = onlyOccurrence(record);
    assertEquals(
        "gbif:dwcdp:occurrence:occurrence_pk:O1",
        occurrence.get(DwcTerm.occurrenceID.qualifiedName()));
    assertNoSyntheticValues(record);
  }

  @Test
  void sameOccurrenceRemainsParentScopedThroughExtensionMaterializationBoundary() {
    Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
    tables.put(
        "event",
        spark.createDataFrame(
            List.of(row("E1", "EV1", "Sensor"), row("E2", "EV2", "Sensor")),
            new StructType()
                .add("event_pk", DataTypes.StringType)
                .add("eventID", DataTypes.StringType)
                .add("eventType", DataTypes.StringType)));
    tables.put("occurrence", occurrences(row("O1", "occ-1", null)));
    tables.put(
        "material",
        materials(
            row("M1", "E1", null, null, "CAT-E1", null),
            row("M2", "E2", null, null, "CAT-E2", null)));
    tables.put(
        "identification",
        identifications(row("I1", "M1", null, null, "O1"), row("I2", "M2", null, null, "O1")));
    TableLoader loader = resource -> Optional.ofNullable(tables.get(resource));

    NestedExtensionContext context = EventOccurrenceNestedContextMapping.context(graph);
    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery(graph, context).discover(loader);
    assertEquals(2L, discovery.ownership().count());
    assertEquals(2L, discovery.uniqueContext().count());

    TableLoader scoped = SparkNestedContextLoader.loader(loader, context, discovery).orElseThrow();
    String contextLink =
        SparkInternalColumns.nestedContextLink(
            context.rowContextLink().column(), context.contextRowLink().column());
    List<Row> scopedOccurrences =
        scoped
            .load("occurrence")
            .orElseThrow()
            .select("occurrence_pk", "event_fk", contextLink)
            .orderBy("event_fk")
            .collectAsList();
    assertEquals(2, scopedOccurrences.size());

    Row firstScoped = scopedOccurrences.get(0);
    Row secondScoped = scopedOccurrences.get(1);
    String firstOccurrencePk = firstScoped.getAs("occurrence_pk");
    String firstEventFk = firstScoped.getAs("event_fk");
    String firstContextLink = firstScoped.getAs(contextLink);
    String secondOccurrencePk = secondScoped.getAs("occurrence_pk");
    String secondEventFk = secondScoped.getAs("event_fk");
    String secondContextLink = secondScoped.getAs(contextLink);
    assertEquals("O1", firstOccurrencePk);
    assertEquals("E1", firstEventFk);
    assertEquals("O1", secondOccurrencePk);
    assertEquals("E2", secondEventFk);
    assertFalse(firstContextLink.equals(secondContextLink));

    CompiledMapping compiled = new MappingCompiler(graph).compile(plan());
    CompiledExtension occurrenceExtension =
        compiled.extensions().stream()
            .filter(extension -> extension.rowType().equals(OccurrenceMapping.ROW_TYPE_OCCURRENCE))
            .findFirst()
            .orElseThrow();
    ExtensionMaterializationResult materialized =
        new SparkExtensionMaterializer(graph).materialize(scoped, occurrenceExtension);

    String catalogColumn =
        Optional.ofNullable(materialized.targetColumns().get(DwcTerm.catalogNumber.qualifiedName()))
            .orElseThrow();
    List<Row> materializedRows =
        materialized
            .dataset()
            .select(materialized.parentKeyColumn(), materialized.rowKeyColumn(), catalogColumn)
            .orderBy(materialized.parentKeyColumn())
            .collectAsList();
    assertEquals(2, materializedRows.size(), materialized.dataset().queryExecution().toString());

    Row firstMaterialized = materializedRows.get(0);
    Row secondMaterialized = materializedRows.get(1);
    String firstParent = firstMaterialized.getAs(materialized.parentKeyColumn());
    String firstRow = firstMaterialized.getAs(materialized.rowKeyColumn());
    String firstCatalog = firstMaterialized.getAs(catalogColumn);
    String secondParent = secondMaterialized.getAs(materialized.parentKeyColumn());
    String secondRow = secondMaterialized.getAs(materialized.rowKeyColumn());
    String secondCatalog = secondMaterialized.getAs(catalogColumn);
    assertEquals("E1", firstParent);
    assertEquals("O1", firstRow);
    assertEquals("CAT-E1", firstCatalog);
    assertEquals("E2", secondParent);
    assertEquals("O1", secondRow);
    assertEquals("CAT-E2", secondCatalog);
  }

  @Test
  void actualNestedAttachmentMethodKeepsOneRowPerOwningCore() throws Exception {
    Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
    tables.put(
        "event",
        spark.createDataFrame(
            List.of(row("E1", "EV1", "Sensor"), row("E2", "EV2", "Sensor")),
            new StructType()
                .add("event_pk", DataTypes.StringType)
                .add("eventID", DataTypes.StringType)
                .add("eventType", DataTypes.StringType)));
    tables.put("occurrence", occurrences(row("O1", "occ-1", null)));
    tables.put(
        "material",
        materials(
            row("M1", "E1", null, null, "CAT-E1", null),
            row("M2", "E2", null, null, "CAT-E2", null)));
    tables.put(
        "identification",
        identifications(row("I1", "M1", null, null, "O1"), row("I2", "M2", null, null, "O1")));
    TableLoader loader = resource -> Optional.ofNullable(tables.get(resource));

    MappingPlan mappingPlan = plan();
    NestedExtensionContext context = EventOccurrenceNestedContextMapping.context(graph);
    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery(graph, context).discover(loader);
    TableLoader scoped = SparkNestedContextLoader.loader(loader, context, discovery).orElseThrow();

    CompiledMapping compiled = new MappingCompiler(graph).compile(mappingPlan);
    CompiledExtension occurrenceExtension =
        compiled.extensions().stream()
            .filter(extension -> extension.rowType().equals(OccurrenceMapping.ROW_TYPE_OCCURRENCE))
            .findFirst()
            .orElseThrow();
    ExtensionMaterializationResult materialized =
        new SparkExtensionMaterializer(graph).materialize(scoped, occurrenceExtension);

    SparkExtendedRecordExecutor executor = new SparkExtendedRecordExecutor(graph);
    java.lang.reflect.Method attachNestedRows =
        SparkExtendedRecordExecutor.class.getDeclaredMethod(
            "attachNestedRows",
            TableLoader.class,
            CompiledMapping.class,
            ExtensionMaterializationResult.class,
            Dataset.class,
            NestedExtensionContext.class);
    attachNestedRows.setAccessible(true);
    @SuppressWarnings("unchecked")
    Dataset<Row> attached =
        (Dataset<Row>)
            attachNestedRows.invoke(
                executor, loader, compiled, materialized, discovery.ownership(), context);

    List<Row> rows =
        attached
            .select(
                "__dwca_core_id",
                materialized.rowKeyColumn(),
                materialized.columnName(DwcTerm.catalogNumber.qualifiedName()))
            .orderBy("__dwca_core_id")
            .collectAsList();
    assertEquals(
        2,
        rows.size(),
        "attachNestedRows() must return exactly one parent-scoped occurrence per core: "
            + rows
            + "\n"
            + attached.queryExecution());

    Row first = rows.get(0);
    Row second = rows.get(1);
    String firstCore = first.getAs("__dwca_core_id");
    String firstRow = first.getAs(materialized.rowKeyColumn());
    String firstCatalog =
        first.getAs(materialized.columnName(DwcTerm.catalogNumber.qualifiedName()));
    String secondCore = second.getAs("__dwca_core_id");
    String secondRow = second.getAs(materialized.rowKeyColumn());
    String secondCatalog =
        second.getAs(materialized.columnName(DwcTerm.catalogNumber.qualifiedName()));
    assertEquals("EV1", firstCore);
    assertEquals("O1", firstRow);
    assertEquals("CAT-E1", firstCatalog);
    assertEquals("EV2", secondCore);
    assertEquals("O1", secondRow);
    assertEquals("CAT-E2", secondCatalog);
  }

  @Test
  void sameOccurrenceDiscoveredBeneathMultipleEventsRemainsIsolatedPerEvent() {
    Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
    tables.put(
        "event",
        spark.createDataFrame(
            List.of(row("E1", "EV1", "Sensor"), row("E2", "EV2", "Sensor")),
            new StructType()
                .add("event_pk", DataTypes.StringType)
                .add("eventID", DataTypes.StringType)
                .add("eventType", DataTypes.StringType)));
    tables.put("occurrence", occurrences(row("O1", "occ-1", null)));
    tables.put(
        "material",
        materials(
            row("M1", "E1", null, null, "CAT-E1", null),
            row("M2", "E2", null, null, "CAT-E2", null)));
    tables.put(
        "identification",
        identifications(row("I1", "M1", null, null, "O1"), row("I2", "M2", null, null, "O1")));
    TableLoader loader = resource -> Optional.ofNullable(tables.get(resource));

    Map<String, ExtendedRecord> records =
        new SparkExtendedRecordExecutor(graph)
            .execute(loader, plan()).collectAsList().stream()
                .collect(
                    java.util.stream.Collectors.toMap(ExtendedRecord::getId, record -> record));

    assertEquals(2, records.size());
    assertEquals(
        "CAT-E1", onlyOccurrence(records.get("EV1")).get(DwcTerm.catalogNumber.qualifiedName()));
    assertEquals(
        "CAT-E2", onlyOccurrence(records.get("EV2")).get(DwcTerm.catalogNumber.qualifiedName()));
    assertNoSyntheticValues(records.get("EV1"));
    assertNoSyntheticValues(records.get("EV2"));
  }

  private void assertNoSyntheticValues(ExtendedRecord record) {
    record.getCoreTerms().values().forEach(this::assertNotSynthetic);
    record.getExtensions().values().stream()
        .flatMap(List::stream)
        .flatMap(values -> values.values().stream())
        .forEach(this::assertNotSynthetic);
  }

  private void assertNotSynthetic(String value) {
    assertFalse(value.startsWith("urn:gbif:dwcdp:nested-context:"));
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
            ValueExpression.firstNonBlank(
                ValueExpression.field(event.field("eventID")),
                ValueExpression.concat(
                    ValueExpression.literal("gbif:dwcdp:event:event_pk:"),
                    ValueExpression.field(event.field("event_pk")))))
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
