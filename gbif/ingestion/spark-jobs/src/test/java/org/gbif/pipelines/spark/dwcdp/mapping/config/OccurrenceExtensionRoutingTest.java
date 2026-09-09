package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.junit.jupiter.api.Test;

class OccurrenceExtensionRoutingTest {

  private final SchemaGraph graph = new DwcDpSchemaLoader().current();

  @Test
  void eventCoreRoutesOnlyOccurrenceOwnedUnionExtensionRows() {
    MappingPlan plan = EventDwcaMapping.current(graph);

    assertRouted(
        plan,
        MultimediaMapping.ROW_TYPE_MULTIMEDIA,
        "occurrence-media-promoted-to-event",
        "material-media-promoted-to-event");
    assertRouted(
        plan,
        AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT,
        "occurrence-assertions-for-event",
        "material-assertions-for-event-occurrence",
        "material-nucleotide-analysis-assertions-for-event-occurrence",
        "material-molecular-protocol-assertions-for-event-occurrence");
    assertRouted(
        plan,
        IdentificationMapping.ROW_TYPE_IDENTIFICATION,
        "occurrence-identification-history-for-event",
        "material-dna-analysis-identifications-for-event-occurrence",
        "material-dna-sequence-identifications-for-event-occurrence");
    assertRouted(
        plan,
        IdentifierMapping.ROW_TYPE_IDENTIFIER,
        "occurrence-identifiers-for-event",
        "material-identifiers-for-event-occurrence");
    assertRouted(
        plan,
        ReferenceMapping.ROW_TYPE_REFERENCE,
        "occurrence-references-for-event",
        "occurrence-protocol-references-for-event",
        "material-references-for-event-occurrence",
        "material-protocol-references-for-event-occurrence",
        "identification-references-for-event-occurrence",
        "material-molecular-protocol-references-for-event-occurrence",
        "material-dna-analysis-identification-references-for-event-occurrence",
        "material-dna-sequence-identification-references-for-event-occurrence");

    assertNotRouted(plan, MultimediaMapping.ROW_TYPE_MULTIMEDIA, "event-media");
    assertNotRouted(
        plan, MultimediaMapping.ROW_TYPE_MULTIMEDIA, "chronometric-age-media-for-event");
    assertNotRouted(
        plan,
        AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT,
        "event-assertions",
        "survey-assertions-for-event",
        "chronometric-age-assertions-for-event");
    assertNotRouted(plan, IdentifierMapping.ROW_TYPE_IDENTIFIER, "event-identifiers");
    assertNotRouted(plan, ReferenceMapping.ROW_TYPE_REFERENCE, "event-references");
  }

  private static void assertRouted(MappingPlan plan, String rowType, String... fragmentNames) {
    for (String fragmentName : fragmentNames) {
      assertTrue(hasOccurrenceId(fragment(plan, rowType, fragmentName)), fragmentName);
    }
  }

  private static void assertNotRouted(MappingPlan plan, String rowType, String... fragmentNames) {
    for (String fragmentName : fragmentNames) {
      assertFalse(hasOccurrenceId(fragment(plan, rowType, fragmentName)), fragmentName);
    }
  }

  private static boolean hasOccurrenceId(ExtensionFragment fragment) {
    return fragment.fields().stream()
        .anyMatch(field -> field.targetTerm().equals(DwcTerm.occurrenceID.qualifiedName()));
  }

  private static ExtensionFragment fragment(MappingPlan plan, String rowType, String name) {
    Set<ExtensionFragment> matches =
        plan.extensions().stream()
            .filter(extension -> extension.rowType().equals(rowType))
            .flatMap(extension -> extension.fragments().stream())
            .filter(fragment -> fragment.name().equals(name))
            .collect(Collectors.toSet());
    if (matches.size() != 1) {
      throw new AssertionError(
          "Expected one fragment named " + name + " but found " + matches.size());
    }
    return matches.iterator().next();
  }
}
