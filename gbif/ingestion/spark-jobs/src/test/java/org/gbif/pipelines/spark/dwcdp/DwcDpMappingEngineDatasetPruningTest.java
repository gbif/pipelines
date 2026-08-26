package org.gbif.pipelines.spark.dwcdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingInputRequirements;
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventDwcaMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DwcDpMappingEngineDatasetPruningTest {

  private SparkSession spark;

  @BeforeAll
  void setupSpark() {
    spark =
        SparkTestSession.createBuilder()
            .appName("DwcDpMappingEngineDatasetPruningTest")
            .getOrCreate();
  }

  @AfterAll
  void teardownSpark() {
    spark.stop();
  }

  @Test
  void eventOnlyDatasetPrunesUnreachableBranchesBeforeInputAnalysis() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage =
        DataPackageFixtures.withEvent("event_pk", "eventID", "eventDate", "country");
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    CompiledMapping scoped = engine.compile(plan, dataPackage);
    MappingInputRequirements requirements = engine.inputRequirements(plan, dataPackage);

    assertTrue(scoped.extensions().isEmpty());
    assertEquals(Set.of("event"), requirements.resources().keySet());
    assertTrue(requirements.resource("event").columns().contains("event_pk"));
    assertTrue(requirements.resource("event").columns().contains("eventID"));
    assertTrue(requirements.resource("event").columns().contains("eventDate"));
    assertTrue(requirements.resource("event").columns().contains("country"));
  }

  @Test
  void eventOccurrenceDatasetKeepsOnlyReachableOccurrenceExtensionBranches() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage = DataPackageFixtures.withEventAndOccurrence();
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    CompiledMapping scoped = engine.compile(plan, dataPackage);
    MappingInputRequirements requirements = engine.inputRequirements(plan, dataPackage);

    assertEquals(Set.of("event", "occurrence"), requirements.resources().keySet());
    assertFalse(scoped.extensions().isEmpty());
    assertTrue(
        scoped.extensions().stream()
            .flatMap(extension -> extension.fragments().stream())
            .allMatch(
                fragment ->
                    fragment.sourceResource().equals("event")
                        || fragment.sourceResource().equals("occurrence")));

    assertFalse(requirements.usesResource("media"));
    assertFalse(requirements.usesResource("agent"));
    assertFalse(requirements.usesResource("protocol"));
    assertFalse(requirements.usesResource("material"));
  }

  @Test
  void datasetPlanUsesSamePrunedCompiledWinnerSetAsExecution() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage = DataPackageFixtures.withEvent("event_pk", "eventID", "eventDate");
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    String rendered = engine.targetPlanDetailed(plan, dataPackage);

    assertTrue(rendered.contains("View: dataset / detailed"));
    assertTrue(rendered.contains("event.eventDate"));
    assertFalse(rendered.contains("protocol.protocolDescription"));
    assertFalse(rendered.contains("agent.preferredAgentName"));
  }

  @Test
  void datasetColumnsAreHardExecutionBoundaries() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage = DataPackageFixtures.withEvent("event_pk", "eventID");
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    CompiledMapping scoped = engine.compile(plan, dataPackage);
    MappingInputRequirements requirements = engine.inputRequirements(plan, dataPackage);

    assertFalse(
        scoped.coreTargets().stream()
            .flatMap(producer -> producer.sources().stream())
            .anyMatch(source -> source.field().column().equals("eventDate")));
    assertFalse(requirements.resource("event").columns().contains("eventDate"));
  }

  @Test
  void missingDeclaredFilterColumnPrunesAcceptedIdentificationBranch() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage =
        DataPackageFixtures.withEventOccurrenceAndIdentificationWithoutAcceptedFlag();
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    CompiledMapping scoped = engine.compile(plan, dataPackage);
    MappingInputRequirements requirements = engine.inputRequirements(plan, dataPackage);

    assertFalse(
        requirements.resource("identification").columns().contains("isAcceptedIdentification"));
    assertFalse(
        scoped.extensions().stream()
            .flatMap(extension -> extension.fragments().stream())
            .flatMap(fragment -> fragment.relations().stream())
            .anyMatch(
                relation ->
                    relation.filter().requiredColumns().contains("isAcceptedIdentification")));
  }

  @Test
  void missingAcceptedIdentificationFilterColumnCannotReachSparkExecution() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage =
        DataPackageFixtures.withEventOccurrenceAndIdentificationWithoutAcceptedFlag();
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    Dataset<Row> event =
        spark.createDataFrame(
            List.of(RowFactory.create("EPK-1", "EV1")),
            new StructType()
                .add("event_pk", DataTypes.StringType)
                .add("eventID", DataTypes.StringType));
    Dataset<Row> occurrence =
        spark.createDataFrame(
            List.of(RowFactory.create("OPK-1", "OCC1", "EPK-1", "present")),
            new StructType()
                .add("occurrence_pk", DataTypes.StringType)
                .add("occurrenceID", DataTypes.StringType)
                .add("event_fk", DataTypes.StringType)
                .add("occurrenceStatus", DataTypes.StringType));
    Dataset<Row> identification =
        spark.createDataFrame(
            List.of(RowFactory.create("IPK-1", "ID1", "OPK-1", "Parus major")),
            new StructType()
                .add("identification_pk", DataTypes.StringType)
                .add("identificationID", DataTypes.StringType)
                .add("occurrence_fk", DataTypes.StringType)
                .add("scientificName", DataTypes.StringType));

    TableLoader loader =
        resource ->
            switch (resource) {
              case "event" -> Optional.of(event);
              case "occurrence" -> Optional.of(occurrence);
              case "identification" -> Optional.of(identification);
              default -> Optional.empty();
            };

    // Regression: dataset specialization must remove the accepted-identification relation before
    // Spark sees its isAcceptedIdentification predicate. Execution itself is the assertion.
    assertEquals(1, engine.execute(loader, plan, dataPackage).collectAsList().size());
  }

  @Test
  void optionalProtocolResourceDoesNotRemoveAssertionExtension() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage = DataPackageFixtures.withEventAndAssertion();
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    CompiledMapping scoped = engine.compile(plan, dataPackage);

    assertTrue(
        scoped.extensions().stream()
            .anyMatch(
                extension ->
                    extension
                        .rowType()
                        .equals(
                            org.gbif.pipelines.spark.dwcdp.mapping.config.AssertionMapping
                                .ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT)));
  }

  @Test
  void humboldtTargetFragmentFallsBackToPhysicalSurveyRowsWhenTargetTablesAreAbsent() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage = DataPackageFixtures.withEventAndSurvey();
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    CompiledMapping scoped = engine.compile(plan, dataPackage);

    var humboldt =
        scoped.extensions().stream()
            .filter(
                extension ->
                    extension
                        .rowType()
                        .equals(
                            org.gbif.pipelines.spark.dwcdp.mapping.config.HumboldtMapping
                                .ROW_TYPE_HUMBOLDT))
            .findFirst()
            .orElseThrow();
    var base =
        humboldt.fragments().stream()
            .filter(fragment -> fragment.name().equals("humboldt-survey-targets"))
            .findFirst()
            .orElseThrow();
    assertTrue(base.rowIdentity().isEmpty());
    assertTrue(base.relations().isEmpty());
  }

  @Test
  void datasetPruningPreservesPositionalAggregationSourceShape() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage = DataPackageFixtures.withEvent("event_pk", "eventID");
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    CompiledMapping scoped = engine.compile(plan, dataPackage);

    var georeference =
        scoped.coreTargetMerges().stream()
            .filter(
                merge -> merge.targetTerm().equals(DwcTerm.georeferenceProtocol.qualifiedName()))
            .flatMap(merge -> merge.producers().stream())
            .filter(
                producer ->
                    producer.aggregation() instanceof ValueAggregation.PreferredLabeledOrFallback)
            .findFirst()
            .orElseThrow();

    assertTrue(georeference.sources().size() >= 4);
  }

  @Test
  void humboldtEnrichPlanKeepsExactlyOneRowDefiningFragmentWhenTargetsExist() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage =
        DataPackageFixtures.withEventOccurrenceOrganismMediaAssertionAndSurvey();
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    CompiledMapping scoped = engine.compile(plan, dataPackage);

    var humboldt =
        scoped.extensions().stream()
            .filter(
                extension ->
                    extension
                        .rowType()
                        .equals(
                            org.gbif.pipelines.spark.dwcdp.mapping.config.HumboldtMapping
                                .ROW_TYPE_HUMBOLDT))
            .findFirst()
            .orElseThrow();

    assertEquals(
        1,
        humboldt.fragments().stream()
            .filter(fragment -> fragment.rowIdentity().isPresent())
            .count());
    assertTrue(
        humboldt.fragments().stream()
            .anyMatch(fragment -> fragment.name().equals("humboldt-survey-targets")));
  }
}
