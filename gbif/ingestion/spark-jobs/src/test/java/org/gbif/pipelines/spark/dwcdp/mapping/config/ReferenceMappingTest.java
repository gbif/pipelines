package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionRowComposition;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.junit.jupiter.api.Test;

class ReferenceMappingTest {

  private final SchemaGraph graph = new DwcDpSchemaLoader().current();

  @Test
  void surveyReferencesArePromotedThroughSurveyToEvent() {
    ExtensionFragment fragment = ReferenceMapping.surveyReferencesForEvent(graph);

    assertEquals("event", fragment.sourceResource());
    assertEquals("event_pk", fragment.scopeKeyColumn().orElseThrow());
    assertEquals(
        List.of("survey", "survey-reference", "bibliographic-resource"),
        fragment.relations().stream()
            .map(relation -> relation.targetResource())
            .collect(Collectors.toList()));
    assertEquals("reference_pk", fragment.rowIdentity().orElseThrow().column());
  }


  @Test
  void referenceFragmentsCompileThroughTheirJunctionResources() {
    assertCompiledPath(
        ReferenceMapping.surveyReferencesForEvent(graph),
        List.of("survey", "survey-reference", "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.eventSamplingProtocolReferences(graph),
        List.of("protocol", "protocol-reference", "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.eventProtocolReferences(graph),
        List.of("event-protocol", "protocol", "protocol-reference", "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.surveySamplingProtocolReferencesForEvent(graph),
        List.of("survey", "protocol", "protocol-reference", "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.surveyProtocolReferencesForEvent(graph),
        List.of(
            "survey",
            "survey-protocol",
            "protocol",
            "protocol-reference",
            "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.chronometricAgeReferencesForEvent(graph),
        List.of("chronometric-age", "chronometric-age-reference", "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.materialReferencesForOccurrence(graph),
        List.of("material", "material-reference", "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.materialProtocolReferencesForOccurrence(graph),
        List.of(
            "material",
            "material-protocol",
            "protocol",
            "protocol-reference",
            "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.chronometricAgeConversionProtocolReferencesForOccurrence(graph),
        List.of(
            "event",
            "chronometric-age",
            "protocol",
            "protocol-reference",
            "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.identificationReferencesForOccurrence(graph),
        List.of("identification", "identification-reference", "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.molecularProtocolReferencesForEvent(graph),
        List.of(
            "nucleotide-analysis",
            "molecular-protocol",
            "molecular-protocol-reference",
            "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.dnaAnalysisIdentificationReferencesForEvent(graph),
        List.of(
            "nucleotide-analysis",
            "identification",
            "identification-reference",
            "bibliographic-resource"));
    assertCompiledPath(
        ReferenceMapping.dnaSequenceIdentificationReferencesForOccurrence(graph),
        List.of(
            "material",
            "nucleotide-analysis",
            "nucleotide-sequence",
            "identification",
            "identification-reference",
            "bibliographic-resource"));
  }

  @Test
  void literatureReferenceProjectionUsesOnlySupportedTargetTerms() {
    ExtensionFragment fragment = ReferenceMapping.eventReferences(graph);

    Set<String> targets =
        fragment.fields().stream().map(field -> field.targetTerm()).collect(Collectors.toSet());

    assertEquals(
        Set.of(
            DcTerm.identifier.qualifiedName(),
            DcTerm.bibliographicCitation.qualifiedName(),
            DcTerm.title.qualifiedName(),
            DcTerm.creator.qualifiedName(),
            DcTerm.date.qualifiedName(),
            DcTerm.source.qualifiedName(),
            DcTerm.description.qualifiedName(),
            DcTerm.type.qualifiedName()),
        targets);
  }

  @Test
  void canonicalPlansContainGeneralizedReferenceExtension() {
    MappingPlan eventPlan = EventDwcaMapping.current(graph);
    ExtensionMapping eventReferences = referenceExtension(eventPlan);
    assertEquals(
        Set.of(
            "event-references",
            "event-sampling-protocol-references",
            "event-georeference-protocol-references",
            "event-protocol-references",
            "survey-references-for-event",
            "survey-sampling-protocol-references-for-event",
            "survey-sampling-effort-protocol-references-for-event",
            "survey-protocol-references-for-event",
            "chronometric-age-references-for-event",
            "chronometric-age-protocol-references-for-event",
            "chronometric-age-conversion-protocol-references-for-event",
            "event-molecular-protocol-references",
            "event-dna-analysis-identification-references",
            "event-dna-sequence-identification-references"),
        eventReferences.fragments().stream()
            .map(ExtensionFragment::name)
            .collect(Collectors.toSet()));

    MappingPlan occurrencePlan = OccurrenceDwcaMapping.current(graph);
    ExtensionMapping occurrenceReferences = referenceExtension(occurrencePlan);
    assertEquals(
        Set.of(
            "occurrence-references",
            "occurrence-protocol-references",
            "material-references-for-occurrence",
            "material-protocol-references-for-occurrence",
            "identification-references-for-occurrence",
            "chronometric-age-references-for-occurrence",
            "chronometric-age-protocol-references-for-occurrence",
            "chronometric-age-conversion-protocol-references-for-occurrence",
            "material-molecular-protocol-references-for-occurrence",
            "material-dna-analysis-identification-references-for-occurrence",
            "material-dna-sequence-identification-references-for-occurrence"),
        occurrenceReferences.fragments().stream()
            .map(ExtensionFragment::name)
            .collect(Collectors.toSet()));

    assertTrue(eventReferences.fragments().size() > 1);
    assertTrue(occurrenceReferences.fragments().size() > 1);
  }


  private void assertCompiledPath(ExtensionFragment fragment, List<String> expectedResources) {
    ExtensionMapping extension =
        new ExtensionMapping(
            ReferenceMapping.ROW_TYPE_REFERENCE, ExtensionRowComposition.UNION, List.of(fragment));
    CompiledExtension compiled = new MappingCompiler(graph).compile(extension);

    assertEquals(
        expectedResources,
        compiled.fragments().get(0).relations().stream()
            .map(relation -> relation.relation().targetResource())
            .collect(Collectors.toList()));
  }

  private static ExtensionMapping referenceExtension(MappingPlan plan) {
    return plan.extensions().stream()
        .filter(extension -> extension.rowType().equals(ReferenceMapping.ROW_TYPE_REFERENCE))
        .findFirst()
        .orElseThrow();
  }
}
