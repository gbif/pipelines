package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionRowComposition;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.junit.jupiter.api.Test;

class IdentificationDnaMappingTest {

  private final SchemaGraph graph = new DwcDpSchemaLoader().current();

  @Test
  void eventDnaIdentificationsFollowAnalysisAndSequenceOwnership() {
    ExtensionFragment analysis = IdentificationMapping.eventDnaAnalysisIdentifications(graph);
    ExtensionFragment sequence = IdentificationMapping.eventDnaSequenceIdentifications(graph);

    assertEquals(
        List.of("nucleotide-analysis", "identification", "agent"), relationTargets(analysis));
    assertEquals(
        List.of("nucleotide-analysis", "nucleotide-sequence", "identification", "agent"),
        relationTargets(sequence));
    assertEquals(Set.of("materialEntity_fk"), analysis.relations().get(0).filter().requiredColumns());
    assertEquals(Set.of("occurrence_fk"), analysis.relations().get(1).filter().requiredColumns());
    assertEquals(
        Set.of("occurrence_fk", "nucleotideAnalysis_fk"),
        sequence.relations().get(2).filter().requiredColumns());

    assertCompiled(analysis);
    assertCompiled(sequence);
  }

  @Test
  void occurrenceDnaIdentificationsFollowEvidenceMaterialOwnership() {
    ExtensionFragment analysis =
        IdentificationMapping.occurrenceDnaAnalysisIdentifications(graph);
    ExtensionFragment sequence =
        IdentificationMapping.occurrenceDnaSequenceIdentifications(graph);

    assertEquals(
        List.of("material", "nucleotide-analysis", "identification", "agent"),
        relationTargets(analysis));
    assertEquals(
        List.of(
            "material", "nucleotide-analysis", "nucleotide-sequence", "identification", "agent"),
        relationTargets(sequence));
    assertEquals("occurrence_pk", analysis.scopeKeyColumn().orElseThrow());
    assertEquals("identification_pk", analysis.rowIdentity().orElseThrow().column());

    assertCompiled(analysis);
    assertCompiled(sequence);
  }

  @Test
  void canonicalPlansUnionIndependentIdentificationOwners() {
    ExtensionMapping eventIdentifications = identificationExtension(EventDwcaMapping.current(graph));
    assertEquals(ExtensionRowComposition.UNION, eventIdentifications.rowComposition());
    assertEquals(
        Set.of("event-dna-analysis-identifications", "event-dna-sequence-identifications"),
        fragmentNames(eventIdentifications));

    ExtensionMapping occurrenceIdentifications =
        identificationExtension(OccurrenceDwcaMapping.current(graph));
    assertEquals(ExtensionRowComposition.UNION, occurrenceIdentifications.rowComposition());
    assertEquals(
        Set.of(
            "occurrence-identification-history",
            "material-dna-analysis-identifications-for-occurrence",
            "material-dna-sequence-identifications-for-occurrence"),
        fragmentNames(occurrenceIdentifications));
  }

  private void assertCompiled(ExtensionFragment fragment) {
    ExtensionMapping extension =
        new ExtensionMapping(
            IdentificationMapping.ROW_TYPE_IDENTIFICATION,
            ExtensionRowComposition.UNION,
            List.of(fragment));
    CompiledExtension compiled = new MappingCompiler(graph).compile(extension);
    assertEquals(1, compiled.fragments().size());
    assertTrue(compiled.fragments().get(0).rowIdentity().isPresent());
  }

  private static List<String> relationTargets(ExtensionFragment fragment) {
    return fragment.relations().stream().map(relation -> relation.targetResource()).toList();
  }

  private static ExtensionMapping identificationExtension(MappingPlan plan) {
    return plan.extensions().stream()
        .filter(extension -> extension.rowType().equals(IdentificationMapping.ROW_TYPE_IDENTIFICATION))
        .findFirst()
        .orElseThrow();
  }

  private static Set<String> fragmentNames(ExtensionMapping extension) {
    return extension.fragments().stream()
        .map(ExtensionFragment::name)
        .collect(Collectors.toSet());
  }
}
