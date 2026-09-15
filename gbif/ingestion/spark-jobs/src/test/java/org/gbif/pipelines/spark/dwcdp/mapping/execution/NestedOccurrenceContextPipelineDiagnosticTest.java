package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventOccurrenceNestedContextMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.config.OccurrenceMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.Mapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.NestedExtensionContext;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.Projection;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

/**
 * Diagnostic coverage for Event-core Occurrence discovery and contextual Material enrichment.
 *
 * <p>These tests intentionally stop at successive execution boundaries. When one fails, its failure
 * message includes the Spark query execution so the first broken boundary is visible without
 * recursively expanding Catalyst constraints.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(30)
class NestedOccurrenceContextPipelineDiagnosticTest {
  private SparkSession spark;
  private SchemaGraph graph;
  private NestedExtensionContext context;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName(getClass().getSimpleName()).getOrCreate();
    graph = new DwcDpSchemaLoader().current();
    context = EventOccurrenceNestedContextMapping.context(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void indirectMaterialIdentificationDiscoveryCarriesMaterialIdentity() {
    Fixture fixture = directMaterialIdentificationFixture();

    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery(graph, context).discover(fixture.loader());

    assertTriples(discovery.ownership(), List.of(List.of("E1", "O1")));
    assertTriples(discovery.contextRows(), List.of(List.of("E1", "O1", "M1")));
    assertTriples(discovery.uniqueContext(), List.of(List.of("E1", "O1", "M1")));
  }

  @Test
  void scopedLoaderPreservesPublisherFieldsAndAddsSharedSyntheticContextLink() {
    Fixture fixture = directMaterialIdentificationFixture();
    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery(graph, context).discover(fixture.loader());
    TableLoader scoped =
        SparkNestedContextLoader.loader(fixture.loader(), context, discovery).orElseThrow();

    Dataset<Row> occurrence = scoped.load("occurrence").orElseThrow();
    Dataset<Row> material = scoped.load("material").orElseThrow();
    String link =
        SparkInternalColumns.nestedContextLink(
            context.rowContextLink().column(), context.contextRowLink().column());

    Row occurrenceRow = occurrence.first();
    Row materialRow = material.first();

    String occurrenceId = occurrenceRow.getAs("occurrenceID");
    String eventFk = occurrenceRow.getAs("event_fk");
    String materialPk = materialRow.getAs("materialEntity_pk");
    String catalogNumber = materialRow.getAs("catalogNumber");
    String evidenceForOccurrenceID = materialRow.getAs("evidenceForOccurrenceID");
    assertEquals("occ-1", occurrenceId, diagnostics("occurrence", occurrence));
    assertEquals("E1", eventFk, diagnostics("occurrence", occurrence));
    assertEquals("M1", materialPk, diagnostics("material", material));
    assertEquals("CAT-1", catalogNumber, diagnostics("material", material));
    assertEquals(null, evidenceForOccurrenceID, diagnostics("material", material));

    String occurrenceLink = occurrenceRow.getAs(link);
    String materialLink = materialRow.getAs(link);
    assertNotNull(occurrenceLink, diagnostics("occurrence", occurrence));
    assertEquals(occurrenceLink, materialLink, diagnostics("material", material));
  }

  @Test
  void configuredOccurrenceMaterialPathReconnectsDiscoveredMaterial() {
    Fixture fixture = directMaterialIdentificationFixture();
    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery(graph, context).discover(fixture.loader());
    TableLoader scoped =
        SparkNestedContextLoader.loader(fixture.loader(), context, discovery).orElseThrow();

    ExtensionFragment fragment = OccurrenceMapping.material(graph);
    Mapping navigation =
        new Mapping(
            "diagnostic-" + fragment.name(),
            fragment.sourceResource(),
            fragment.relations(),
            List.of(),
            Projection.none());

    MappingExecutionResult execution =
        new SparkMappingPathExecutor(graph).execute(scoped, navigation);
    SparkPathResult path = execution.pathResult();
    String catalogAlias = alias(path, "material", "catalogNumber");

    List<String> catalogs =
        path.dataset().select(catalogAlias).collectAsList().stream()
            .map(row -> (String) row.getAs(catalogAlias))
            .filter(value -> value != null)
            .distinct()
            .toList();

    assertEquals(
        List.of("CAT-1"),
        catalogs,
        diagnostics("configured material path", path.dataset())
            + "\nmetrics="
            + execution.metrics());
  }

  @Test
  void analysisAndSequenceDiscoveryAlsoCarryMaterialIdentity() {
    assertUniqueContext(materialAnalysisIdentificationFixture(), "M1");
    assertUniqueContext(materialAnalysisSequenceIdentificationFixture(), "M1");
  }

  @Test
  void materialFragmentSurvivesExtensionMaterialization() {
    Fixture fixture = directMaterialIdentificationFixture();
    TableLoader scoped = scopedLoader(fixture);

    CompiledExtension extension =
        compileOccurrenceExtension(
            OccurrenceMapping.directOccurrence(graph), OccurrenceMapping.material(graph));
    ExtensionMaterializationResult result =
        new SparkExtensionMaterializer(graph).materialize(scoped, extension);

    String catalogColumn =
        Optional.ofNullable(
                result
                    .targetColumns()
                    .get(org.gbif.dwc.terms.DwcTerm.catalogNumber.qualifiedName()))
            .orElseThrow(
                () ->
                    new AssertionError(
                        "catalogNumber was not materialized: " + result.targetColumns()));
    List<Row> rows = result.dataset().select(catalogColumn).collectAsList();
    assertEquals(1, rows.size(), diagnostics("materialized material fragment", result.dataset()));
    String catalogNumber = rows.get(0).getAs(catalogColumn);
    assertEquals(
        "CAT-1", catalogNumber, diagnostics("materialized material fragment", result.dataset()));
  }

  @Test
  void materialBasisOfRecordSurvivesExtensionMaterialization() {
    Fixture fixture = directMaterialIdentificationFixture();
    TableLoader scoped = scopedLoader(fixture);

    CompiledExtension extension =
        compileOccurrenceExtension(
            OccurrenceMapping.directOccurrence(graph),
            OccurrenceMapping.materialBasisOfRecord(graph));
    ExtensionMaterializationResult result =
        new SparkExtensionMaterializer(graph).materialize(scoped, extension);

    String borColumn =
        Optional.ofNullable(
                result
                    .targetColumns()
                    .get(org.gbif.dwc.terms.DwcTerm.basisOfRecord.qualifiedName()))
            .orElseThrow(
                () ->
                    new AssertionError(
                        "basisOfRecord was not materialized: " + result.targetColumns()));
    List<Row> rows = result.dataset().select(borColumn).collectAsList();
    assertEquals(1, rows.size(), diagnostics("materialized material BOR", result.dataset()));
    String basisOfRecord = rows.get(0).getAs(borColumn);
    assertEquals(
        "PreservedSpecimen",
        basisOfRecord,
        diagnostics("materialized material BOR", result.dataset()));
  }

  @Test
  void fullOccurrenceExtensionContainsMaterialContributionsBeforeAttachment() {
    Fixture fixture = directMaterialIdentificationFixture();
    TableLoader scoped = scopedLoader(fixture);

    CompiledMapping compiled = new MappingCompiler(graph).compile(fullPlan());
    CompiledExtension extension =
        compiled.extensions().stream()
            .filter(candidate -> candidate.rowType().equals(OccurrenceMapping.ROW_TYPE_OCCURRENCE))
            .findFirst()
            .orElseThrow();
    ExtensionMaterializationResult result =
        new SparkExtensionMaterializer(graph).materialize(scoped, extension);

    String occurrenceIdColumn =
        Optional.ofNullable(
                result.targetColumns().get(org.gbif.dwc.terms.DwcTerm.occurrenceID.qualifiedName()))
            .orElseThrow();
    String catalogColumn =
        Optional.ofNullable(
                result
                    .targetColumns()
                    .get(org.gbif.dwc.terms.DwcTerm.catalogNumber.qualifiedName()))
            .orElseThrow();
    String borColumn =
        Optional.ofNullable(
                result
                    .targetColumns()
                    .get(org.gbif.dwc.terms.DwcTerm.basisOfRecord.qualifiedName()))
            .orElseThrow();

    Row row = result.dataset().select(occurrenceIdColumn, catalogColumn, borColumn).first();
    String occurrenceId = row.getAs(occurrenceIdColumn);
    String catalogNumber = row.getAs(catalogColumn);
    String basisOfRecord = row.getAs(borColumn);

    assertEquals(
        "occ-1", occurrenceId, diagnostics("full extension before attachment", result.dataset()));
    assertEquals(
        "CAT-1", catalogNumber, diagnostics("full extension before attachment", result.dataset()));
    assertEquals(
        "PreservedSpecimen",
        basisOfRecord,
        diagnostics("full extension before attachment", result.dataset()));
  }

  private TableLoader scopedLoader(Fixture fixture) {
    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery(graph, context).discover(fixture.loader());
    return SparkNestedContextLoader.loader(fixture.loader(), context, discovery).orElseThrow();
  }

  private CompiledExtension compileOccurrenceExtension(ExtensionFragment... fragments) {
    MappingPlanBuilder.ExtensionBuilder extension =
        MappingPlanBuilder.mappingPlan("diagnostic-extension", CoreType.EVENT, "event")
            .coreIdentity(
                ValueExpression.firstNonBlank(
                    ValueExpression.field(MappingPath.root(graph, "event").field("eventID")),
                    ValueExpression.concat(
                        ValueExpression.literal("gbif:dwcdp:event:event_pk:"),
                        ValueExpression.field(MappingPath.root(graph, "event").field("event_pk")))))
            .extension(OccurrenceMapping.ROW_TYPE_OCCURRENCE);
    for (ExtensionFragment fragment : fragments) {
      extension.importFragment(fragment);
    }
    CompiledMapping compiled = new MappingCompiler(graph).compile(extension.build());
    return compiled.extensions().get(0);
  }

  private MappingPlan fullPlan() {
    MappingPath event = MappingPath.root(graph, "event");
    return MappingPlanBuilder.mappingPlan("diagnostic-full-extension", CoreType.EVENT, "event")
        .nestedExtensionContext(context)
        .coreIdentity(
            ValueExpression.firstNonBlank(
                ValueExpression.field(event.field("eventID")),
                ValueExpression.concat(
                    ValueExpression.literal("gbif:dwcdp:event:event_pk:"),
                    ValueExpression.field(event.field("event_pk")))))
        .coreField(
            TargetFieldMapping.oneOf(
                org.gbif.dwc.terms.DwcTerm.eventID.qualifiedName(),
                ValueAggregation.firstNonNull(),
                event.field("eventID")))
        .extension(OccurrenceMapping.ROW_TYPE_OCCURRENCE)
        .mergeTarget(
            org.gbif.dwc.terms.DwcTerm.occurrenceID.qualifiedName(),
            ValueAggregation.firstNonNull())
        .mergeTarget(
            org.gbif.dwc.terms.DwcTerm.basisOfRecord.qualifiedName(),
            ValueAggregation.firstNonNull())
        .importFragment(OccurrenceMapping.directOccurrence(graph))
        .importFragment(OccurrenceMapping.material(graph))
        .importFragment(OccurrenceMapping.materialBasisOfRecord(graph))
        .importFragment(OccurrenceMapping.eventBasisOfRecord(graph))
        .importFragment(OccurrenceMapping.defaultBasisOfRecord(graph))
        .build();
  }

  private void assertUniqueContext(Fixture fixture, String expectedMaterial) {
    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery(graph, context).discover(fixture.loader());
    List<Row> rows = discovery.uniqueContext().collectAsList();
    assertEquals(1, rows.size(), diagnostics("uniqueContext", discovery.uniqueContext()));
    String material = rows.get(0).getAs(SparkNestedContextDiscovery.COL_CONTEXT);
    assertEquals(expectedMaterial, material);
  }

  private static void assertTriples(Dataset<Row> dataset, List<List<String>> expected) {
    List<List<String>> actual =
        dataset.collectAsList().stream()
            .map(
                row ->
                    Arrays.stream(dataset.columns())
                        .map(column -> (String) row.getAs(column))
                        .toList())
            .sorted((left, right) -> left.toString().compareTo(right.toString()))
            .toList();
    assertEquals(expected, actual, diagnostics("relation", dataset));
  }

  private static String alias(SparkPathResult result, String resource, String column) {
    return result.aliases().entrySet().stream()
        .filter(entry -> entry.getKey().path().currentResource().equals(resource))
        .filter(entry -> entry.getKey().column().equals(column))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElseThrow(
            () ->
                new AssertionError(
                    "No materialized alias for "
                        + resource
                        + "."
                        + column
                        + "; aliases="
                        + result.aliases()));
  }

  private Fixture directMaterialIdentificationFixture() {
    return fixture(
        occurrences(row("O1", "occ-1", null)),
        materials(row("M1", "E1", null, null, "CAT-1", "preserved")),
        identifications(row("I1", "M1", null, null, "O1")),
        null,
        null);
  }

  private Fixture materialAnalysisIdentificationFixture() {
    return fixture(
        occurrences(row("O1", "occ-1", null)),
        materials(row("M1", "E1", null, null, "CAT-1", "preserved")),
        identifications(row("I1", null, "A1", null, "O1")),
        analyses(row("A1", "M1", null)),
        null);
  }

  private Fixture materialAnalysisSequenceIdentificationFixture() {
    return fixture(
        occurrences(row("O1", "occ-1", null)),
        materials(row("M1", "E1", null, null, "CAT-1", "preserved")),
        identifications(row("I1", null, null, "S1", "O1")),
        analyses(row("A1", "M1", "S1")),
        sequences("S1"));
  }

  private Fixture fixture(
      Dataset<Row> occurrence,
      Dataset<Row> material,
      Dataset<Row> identification,
      Dataset<Row> analysis,
      Dataset<Row> sequence) {
    Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
    tables.put("event", events());
    tables.put("occurrence", occurrence);
    tables.put("material", material);
    tables.put("identification", identification);
    if (analysis != null) {
      tables.put("nucleotide-analysis", analysis);
    }
    if (sequence != null) {
      tables.put("nucleotide-sequence", sequence);
    }
    return new Fixture(resource -> Optional.ofNullable(tables.get(resource)));
  }

  private Dataset<Row> events() {
    return spark.createDataFrame(
        List.of(RowFactory.create("E1", "EV1", "Sensor")),
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

  private static String diagnostics(String label, Dataset<Row> dataset) {
    return "\n[DWCDP-DIAGNOSTIC] " + label + "\n" + dataset.queryExecution();
  }

  private static Row row(Object... values) {
    return RowFactory.create(values);
  }

  private record Fixture(TableLoader loader) {}
}
