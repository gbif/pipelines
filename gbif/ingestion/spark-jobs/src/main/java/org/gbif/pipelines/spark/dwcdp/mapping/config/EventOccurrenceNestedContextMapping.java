package org.gbif.pipelines.spark.dwcdp.mapping.config;

import java.util.List;
import java.util.Optional;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.NestedContextDiscoveryFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.NestedExtensionContext;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

/** Event-core Occurrence ownership and Material context configuration. */
public final class EventOccurrenceNestedContextMapping {

  private EventOccurrenceNestedContextMapping() {}

  public static NestedExtensionContext context(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material = MappingPath.root(graph, "material");

    return new NestedExtensionContext(
        OccurrenceMapping.ROW_TYPE_OCCURRENCE,
        "event",
        "occurrence",
        "material",
        event.field("event_pk"),
        occurrence.field("occurrence_pk"),
        occurrence.field("event_fk"),
        occurrence.field("occurrenceID"),
        material.field("materialEntity_pk"),
        material.field("evidenceForOccurrenceID"),
        Optional.of(DwcTerm.eventID.qualifiedName()),
        discoveryFragments(graph),
        contextualFragments(graph));
  }

  private static List<ExtensionFragment> contextualFragments(SchemaGraph graph) {
    return List.of(
        OccurrenceMapping.material(graph),
        OccurrenceMapping.materialCollectedBy(graph),
        OccurrenceMapping.materialIdentifiedBy(graph),
        OccurrenceMapping.materialCollectorRoles(graph),
        OccurrenceMapping.materialDirectProvenance(graph),
        OccurrenceMapping.materialProvenance(graph),
        OccurrenceMapping.materialGeologicalContext(graph),
        OccurrenceMapping.materialProtocols(graph));
  }

  private static List<NestedContextDiscoveryFragment> discoveryFragments(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");

    MappingPath directOccurrence = event.join("occurrence").via("event_fk").optional().fanOut();
    MappingPath evidenceMaterial =
        directOccurrence.join("material").via("evidenceForOccurrenceID").optional().fanOut();

    MappingPath material = event.join("material").via("collectionEvent_fk").optional().fanOut();
    MappingPath materialIdentification =
        material.join("identification").via("materialEntity_fk").optional().fanOut();
    MappingPath materialOccurrence =
        materialIdentification.join("occurrence").via("occurrence_fk").optional().fanOut();

    MappingPath analysis =
        material.join("nucleotide-analysis").via("materialEntity_fk").optional().fanOut();
    MappingPath analysisIdentification =
        analysis.join("identification").via("nucleotideAnalysis_fk").optional().fanOut();
    MappingPath analysisOccurrence =
        analysisIdentification.join("occurrence").via("occurrence_fk").optional().fanOut();

    MappingPath sequence =
        analysis.join("nucleotide-sequence").via("nucleotideSequence_fk").optional().fanOut();
    MappingPath sequenceIdentification =
        sequence.join("identification").via("nucleotideSequence_fk").optional().fanOut();
    MappingPath sequenceOccurrence =
        sequenceIdentification.join("occurrence").via("occurrence_fk").optional().fanOut();

    return List.of(
        new NestedContextDiscoveryFragment(
            "event-direct-occurrence",
            directOccurrence,
            event.field("event_pk"),
            directOccurrence.field("occurrence_pk"),
            Optional.empty()),
        new NestedContextDiscoveryFragment(
            "event-occurrence-evidence-material",
            evidenceMaterial,
            event.field("event_pk"),
            directOccurrence.field("occurrence_pk"),
            Optional.of(evidenceMaterial.field("materialEntity_pk"))),
        new NestedContextDiscoveryFragment(
            "event-material-identification-occurrence",
            materialOccurrence,
            event.field("event_pk"),
            materialOccurrence.field("occurrence_pk"),
            Optional.of(material.field("materialEntity_pk"))),
        new NestedContextDiscoveryFragment(
            "event-material-analysis-identification-occurrence",
            analysisOccurrence,
            event.field("event_pk"),
            analysisOccurrence.field("occurrence_pk"),
            Optional.of(material.field("materialEntity_pk"))),
        new NestedContextDiscoveryFragment(
            "event-material-analysis-sequence-identification-occurrence",
            sequenceOccurrence,
            event.field("event_pk"),
            sequenceOccurrence.field("occurrence_pk"),
            Optional.of(material.field("materialEntity_pk"))));
  }
}
