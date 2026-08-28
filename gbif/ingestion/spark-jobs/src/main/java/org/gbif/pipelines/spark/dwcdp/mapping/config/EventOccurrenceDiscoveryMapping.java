package org.gbif.pipelines.spark.dwcdp.mapping.config;

import java.util.List;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

/** Declarative Event-core Occurrence ownership paths that do not produce DwC-A target fields. */
public final class EventOccurrenceDiscoveryMapping {

  private EventOccurrenceDiscoveryMapping() {}

  public static List<EventOccurrenceDiscoveryFragment> fragments(SchemaGraph graph) {
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
        new EventOccurrenceDiscoveryFragment(
            "event-direct-occurrence",
            directOccurrence,
            event.field("event_pk"),
            directOccurrence.field("occurrence_pk"),
            Optional.empty()),
        new EventOccurrenceDiscoveryFragment(
            "event-occurrence-evidence-material",
            evidenceMaterial,
            event.field("event_pk"),
            directOccurrence.field("occurrence_pk"),
            Optional.of(evidenceMaterial.field("materialEntity_pk"))),
        new EventOccurrenceDiscoveryFragment(
            "event-material-identification-occurrence",
            materialOccurrence,
            event.field("event_pk"),
            materialOccurrence.field("occurrence_pk"),
            Optional.of(material.field("materialEntity_pk"))),
        new EventOccurrenceDiscoveryFragment(
            "event-material-analysis-identification-occurrence",
            analysisOccurrence,
            event.field("event_pk"),
            analysisOccurrence.field("occurrence_pk"),
            Optional.of(material.field("materialEntity_pk"))),
        new EventOccurrenceDiscoveryFragment(
            "event-material-analysis-sequence-identification-occurrence",
            sequenceOccurrence,
            event.field("event_pk"),
            sequenceOccurrence.field("occurrence_pk"),
            Optional.of(material.field("materialEntity_pk"))));
  }
}
