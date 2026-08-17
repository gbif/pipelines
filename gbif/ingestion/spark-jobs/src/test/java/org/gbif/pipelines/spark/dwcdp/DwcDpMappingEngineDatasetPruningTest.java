package org.gbif.pipelines.spark.dwcdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingInputRequirements;
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventDwcaMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.junit.jupiter.api.Test;

class DwcDpMappingEngineDatasetPruningTest {

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
    DataPackage dataPackage =
        DataPackageFixtures.withEvent("event_pk", "eventID", "eventDate");
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    String rendered = engine.targetPlanDetailed(plan, dataPackage);

    assertTrue(rendered.contains("View: dataset / detailed"));
    assertTrue(rendered.contains("event.eventDate"));
    assertFalse(rendered.contains("protocol.protocolDescription"));
    assertFalse(rendered.contains("agent.preferredAgentName"));
  }
  @Test
  void descriptorColumnsDoNotActAsHardNegativeEvidenceForExecutionPruning() {
    DwcDpMappingEngine engine = DwcDpMappingEngine.currentSchema();
    DataPackage dataPackage = DataPackageFixtures.withEvent("event_pk", "eventID");
    var plan = EventDwcaMapping.current(engine.schemaGraph());

    CompiledMapping scoped = engine.compile(plan, dataPackage);
    MappingInputRequirements requirements = engine.inputRequirements(plan, dataPackage);

    assertTrue(
        scoped.coreTargets().stream()
            .anyMatch(
                producer ->
                    producer.sources().stream()
                        .anyMatch(source -> source.field().column().equals("eventDate"))));
    assertTrue(requirements.resource("event").columns().contains("eventDate"));
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
                    extension.rowType().equals(
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
                    extension.rowType().equals(
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
            .filter(merge -> merge.targetTerm().equals(DwcTerm.georeferenceProtocol.qualifiedName()))
            .flatMap(merge -> merge.producers().stream())
            .filter(
                producer ->
                    producer.aggregation()
                        instanceof ValueAggregation.PreferredLabeledOrFallback)
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
                    extension.rowType().equals(
                        org.gbif.pipelines.spark.dwcdp.mapping.config.HumboldtMapping
                            .ROW_TYPE_HUMBOLDT))
            .findFirst()
            .orElseThrow();

    assertEquals(
        1,
        humboldt.fragments().stream().filter(fragment -> fragment.rowIdentity().isPresent()).count());
    assertTrue(
        humboldt.fragments().stream()
            .anyMatch(fragment -> fragment.name().equals("humboldt-survey-targets")));
  }

}
