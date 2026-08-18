package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionRowComposition;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.junit.jupiter.api.Test;

class AssertionMappingTest {

  private final SchemaGraph graph = new DwcDpSchemaLoader().current();

  @Test
  void surveyAssertionsUseEventOwnershipPath() {
    ExtensionFragment fragment = AssertionMapping.surveyAssertionsForEvent(graph);

    assertEquals("event", fragment.sourceResource());
    assertEquals("event_pk", fragment.scopeKeyColumn().orElseThrow());
    assertEquals(
        List.of("survey", "survey-assertion", "protocol"),
        relationTargets(fragment));
    assertEquals("assertionID", fragment.rowIdentity().orElseThrow().column());
    assertEquals("survey-assertion", fragment.rowIdentity().orElseThrow().path().currentResource());
  }

  @Test
  void eventDnaAssertionsPreserveDirectEventOwnershipRule() {
    ExtensionFragment analysis = AssertionMapping.nucleotideAnalysisAssertionsForEvent(graph);
    ExtensionFragment protocol = AssertionMapping.molecularProtocolAssertionsForEvent(graph);

    assertEquals(
        List.of("nucleotide-analysis", "nucleotide-analysis-assertion", "protocol"),
        relationTargets(analysis));
    assertEquals(
        Set.of("materialEntity_fk"),
        analysis.relations().get(0).filter().requiredColumns());

    assertEquals(
        List.of(
            "nucleotide-analysis",
            "molecular-protocol",
            "molecular-protocol-assertion",
            "protocol"),
        relationTargets(protocol));
    assertEquals(
        Set.of("materialEntity_fk"),
        protocol.relations().get(0).filter().requiredColumns());
  }

  @Test
  void occurrenceDnaAssertionsFollowEvidenceMaterialOwnership() {
    ExtensionFragment analysis =
        AssertionMapping.nucleotideAnalysisAssertionsForOccurrence(graph);
    ExtensionFragment protocol =
        AssertionMapping.molecularProtocolAssertionsForOccurrence(graph);

    assertEquals(
        List.of(
            "material",
            "nucleotide-analysis",
            "nucleotide-analysis-assertion",
            "protocol"),
        relationTargets(analysis));
    assertEquals(
        List.of(
            "material",
            "nucleotide-analysis",
            "molecular-protocol",
            "molecular-protocol-assertion",
            "protocol"),
        relationTargets(protocol));
  }

  @Test
  void canonicalAssertionPlansUnionIndependentAssertionOwners() {
    assertUnionFragments(
        EventDwcaMapping.withAssertions(graph),
        Set.of(
            "event-assertions",
            "survey-assertions-for-event",
            "event-nucleotide-analysis-assertions",
            "event-molecular-protocol-assertions"));

    assertUnionFragments(
        OccurrenceDwcaMapping.withAssertions(graph),
        Set.of(
            "occurrence-assertions",
            "material-assertions-for-occurrence",
            "material-nucleotide-analysis-assertions-for-occurrence",
            "material-molecular-protocol-assertions-for-occurrence"));
  }

  private static List<String> relationTargets(ExtensionFragment fragment) {
    return fragment.relations().stream().map(step -> step.targetResource()).toList();
  }

  private static void assertUnionFragments(MappingPlan plan, Set<String> expectedFragments) {
    ExtensionMapping assertions =
        plan.extensions().stream()
            .filter(
                extension ->
                    extension.rowType().equals(
                        AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT))
            .findFirst()
            .orElseThrow();

    assertEquals(ExtensionRowComposition.UNION, assertions.rowComposition());
    assertEquals(
        expectedFragments,
        assertions.fragments().stream().map(ExtensionFragment::name).collect(Collectors.toSet()));
    assertTrue(assertions.fragments().stream().allMatch(fragment -> fragment.rowIdentity().isPresent()));
  }
}
