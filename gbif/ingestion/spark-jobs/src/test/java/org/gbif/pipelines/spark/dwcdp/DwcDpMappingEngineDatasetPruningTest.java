package org.gbif.pipelines.spark.dwcdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.gbif.pipelines.spark.dwcdp.mapping.execution.MappingExecutionOutput;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageField;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageResource;
import org.gbif.pipelines.spark.util.MapperUtil;
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
    spark = SparkTestSession.createBuilder().appName("DwcDpMappingEngineDatasetPruningTest").getOrCreate();
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

    assertFalse(requirements.resource("identification").columns().contains("isAcceptedIdentification"));
    assertFalse(
        scoped.extensions().stream()
            .flatMap(extension -> extension.fragments().stream())
            .flatMap(fragment -> fragment.relations().stream())
            .anyMatch(
                relation ->
                    relation.filter().requiredColumns().contains("isAcceptedIdentification")));
  }

  @Test
  void suppliedDatasetWithoutAcceptedIdentificationFlagExecutesProductionPath() throws Exception {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage = loadRegressionDataPackage();
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    Map<String, Dataset<Row>> tables = new HashMap<>();
    for (DataPackageResource resource : dataPackage.getResources()) {
      StructType schema = sparkSchema(resource);
      List<Row> rows =
          switch (resource.getName()) {
            case "event" ->
                List.of(row(resource, Map.of("event_pk", "EPK-1", "eventDate", "2024-06-15")));
            case "occurrence" ->
                List.of(
                    row(
                        resource,
                        Map.of(
                            "occurrence_pk", "OPK-1",
                            "occurrenceID", "OCC1",
                            "event_fk", "EPK-1",
                            "occurrenceStatus", "present")));
            case "identification" ->
                List.of(
                    row(
                        resource,
                        Map.of(
                            "identification_pk", "IPK-1",
                            "identificationID", "ID1",
                            "occurrence_fk", "OPK-1",
                            "scientificName", "Parus major",
                            "typeStatus", "",
                            "identifiedBy", "Ada",
                            "dateIdentified", "2024-06-16")));
            default -> List.of();
          };
      tables.put(resource.getName(), spark.createDataFrame(rows, schema));
    }

    TableLoader loader = resource -> Optional.ofNullable(tables.get(resource));

    // This is deliberately the same engine entry point used by DwcDpVerbatimConverter.convert().
    // The regression is only considered covered once Spark materializes the complete Event plan.
    try (MappingExecutionOutput output =
        engine.executeWithMetrics(loader, plan, dataPackage)) {
      assertEquals(1, output.records().collectAsList().size());
    }
  }

  private DataPackage loadRegressionDataPackage() throws Exception {
    try (InputStream input =
        getClass()
            .getResourceAsStream(
                "/dwcdp/regression/missing-accepted-identification-datapackage.json")) {
      if (input == null) {
        throw new IllegalStateException("Regression datapackage fixture not found");
      }
      return MapperUtil.MAPPER.readValue(input, DataPackage.class);
    }
  }

  private StructType sparkSchema(DataPackageResource resource) {
    StructType schema = new StructType();
    for (DataPackageField field : resource.getSchema().getFields()) {
      schema = schema.add(field.getName(), sparkType(field.getType()), true);
    }
    return schema;
  }

  private org.apache.spark.sql.types.DataType sparkType(String type) {
    return switch (type) {
      case "integer" -> DataTypes.IntegerType;
      case "number" -> DataTypes.DoubleType;
      case "boolean" -> DataTypes.BooleanType;
      default -> DataTypes.StringType;
    };
  }

  private Row row(DataPackageResource resource, Map<String, Object> values) {
    Object[] ordered =
        resource.getSchema().getFields().stream()
            .map(field -> values.get(field.getName()))
            .toArray();
    return RowFactory.create(ordered);
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
