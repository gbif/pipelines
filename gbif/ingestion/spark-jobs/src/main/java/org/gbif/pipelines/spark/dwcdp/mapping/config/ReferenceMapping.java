package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.List;
import java.util.Objects;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

/** Declarative mappings for the GBIF Literature Reference extension. */
public final class ReferenceMapping {

  public static final String ROW_TYPE_REFERENCE = "http://rs.gbif.org/terms/1.0/Reference";

  private ReferenceMapping() {}

  public static ExtensionFragment eventReferences(SchemaGraph graph) {
    return references(
        graph, new Spec("event-references", "event-reference", "event_fk", List.of()));
  }

  public static ExtensionFragment eventSamplingProtocolReferences(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-sampling-protocol-references",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.exactlyOne("protocol", "eventProtocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment eventGeoreferenceProtocolReferences(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-georeference-protocol-references",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.exactlyOne("protocol", "georeferenceProtocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment eventProtocolReferences(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-protocol-references",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("event-protocol", "event_fk"),
                OwnershipPathStep.exactlyOne("protocol", "protocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment surveySamplingProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "survey-sampling-protocol-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("survey", "event_fk"),
                OwnershipPathStep.exactlyOne("protocol", "samplingProtocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment surveySamplingEffortProtocolReferencesForEvent(
      SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "survey-sampling-effort-protocol-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("survey", "event_fk"),
                OwnershipPathStep.exactlyOne("protocol", "samplingEffortProtocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment surveyProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "survey-protocol-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("survey", "event_fk"),
                OwnershipPathStep.fanOut("survey-protocol", "survey_fk"),
                OwnershipPathStep.exactlyOne("protocol", "protocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment chronometricAgeProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "chronometric-age-protocol-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("chronometric-age", "event_fk"),
                OwnershipPathStep.exactlyOne("protocol", "chronometricAgeProtocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment chronometricAgeConversionProtocolReferencesForEvent(
      SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "chronometric-age-conversion-protocol-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("chronometric-age", "event_fk"),
                OwnershipPathStep.exactlyOne("protocol", "chronometricAgeConversionProtocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment surveyReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "survey-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("survey", "event_fk"),
                OwnershipPathStep.fanOut("survey-reference", "survey_fk"))));
  }

  public static ExtensionFragment chronometricAgeReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "chronometric-age-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("chronometric-age", "event_fk"),
                OwnershipPathStep.fanOut("chronometric-age-reference", "chronometricAge_fk"))));
  }

  public static ExtensionFragment molecularProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-molecular-protocol-references",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipPathStep.exactlyOne("molecular-protocol", "molecularProtocol_fk"),
                OwnershipPathStep.fanOut("molecular-protocol-reference", "molecularProtocol_fk"))));
  }

  public static ExtensionFragment dnaAnalysisIdentificationReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-dna-analysis-identification-references",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipPathStep.fanOut("identification", "nucleotideAnalysis_fk")
                    .filter(FilterExpression.isNull("occurrence_fk")),
                OwnershipPathStep.fanOut("identification-reference", "identification_fk"))));
  }

  public static ExtensionFragment dnaSequenceIdentificationReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-dna-sequence-identification-references",
            "event",
            "event_pk",
            List.of(
                OwnershipPathStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipPathStep.exactlyOne("nucleotide-sequence", "nucleotideSequence_fk"),
                OwnershipPathStep.fanOut("identification", "nucleotideSequence_fk")
                    .filter(
                        FilterExpression.and(
                            FilterExpression.isNull("occurrence_fk"),
                            FilterExpression.isNull("nucleotideAnalysis_fk"))),
                OwnershipPathStep.fanOut("identification-reference", "identification_fk"))));
  }

  public static ExtensionFragment occurrenceProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        Spec.routed(
            "occurrence-protocol-references-for-event",
            "occurrence",
            "event_fk",
            List.of(
                OwnershipPathStep.exactlyOne("protocol", "occurrenceProtocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment materialProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        Spec.routed(
            "material-protocol-references-for-event-occurrence",
            "occurrence",
            "event_fk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("material-protocol", "materialEntity_fk"),
                OwnershipPathStep.exactlyOne("protocol", "protocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment occurrenceReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        Spec.routed(
            "occurrence-references-for-event",
            "occurrence",
            "event_fk",
            List.of(OwnershipPathStep.fanOut("occurrence-reference", "occurrence_fk"))));
  }

  public static ExtensionFragment materialReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        Spec.routed(
            "material-references-for-event-occurrence",
            "occurrence",
            "event_fk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("material-reference", "materialEntity_fk"))));
  }

  public static ExtensionFragment occurrenceIdentificationReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        Spec.routed(
            "identification-references-for-event-occurrence",
            "occurrence",
            "event_fk",
            List.of(
                OwnershipPathStep.fanOut("identification", "occurrence_fk"),
                OwnershipPathStep.fanOut("identification-reference", "identification_fk"))));
  }

  public static ExtensionFragment occurrenceMolecularProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        Spec.routed(
            "material-molecular-protocol-references-for-event-occurrence",
            "occurrence",
            "event_fk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipPathStep.exactlyOne("molecular-protocol", "molecularProtocol_fk"),
                OwnershipPathStep.fanOut("molecular-protocol-reference", "molecularProtocol_fk"))));
  }

  public static ExtensionFragment occurrenceDnaAnalysisIdentificationReferencesForEvent(
      SchemaGraph graph) {
    return references(
        graph,
        Spec.routed(
            "material-dna-analysis-identification-references-for-event-occurrence",
            "occurrence",
            "event_fk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipPathStep.fanOut("identification", "nucleotideAnalysis_fk")
                    .filter(FilterExpression.isNull("occurrence_fk")),
                OwnershipPathStep.fanOut("identification-reference", "identification_fk"))));
  }

  public static ExtensionFragment occurrenceDnaSequenceIdentificationReferencesForEvent(
      SchemaGraph graph) {
    return references(
        graph,
        Spec.routed(
            "material-dna-sequence-identification-references-for-event-occurrence",
            "occurrence",
            "event_fk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipPathStep.exactlyOne("nucleotide-sequence", "nucleotideSequence_fk"),
                OwnershipPathStep.fanOut("identification", "nucleotideSequence_fk")
                    .filter(
                        FilterExpression.and(
                            FilterExpression.isNull("occurrence_fk"),
                            FilterExpression.isNull("nucleotideAnalysis_fk"))),
                OwnershipPathStep.fanOut("identification-reference", "identification_fk"))));
  }

  public static ExtensionFragment occurrenceProtocolReferences(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "occurrence-protocol-references",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipPathStep.exactlyOne("protocol", "occurrenceProtocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment materialProtocolReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "material-protocol-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("material-protocol", "materialEntity_fk"),
                OwnershipPathStep.exactlyOne("protocol", "protocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment chronometricAgeProtocolReferencesForOccurrence(
      SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "chronometric-age-protocol-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipPathStep.exactlyOne("event", "event_fk"),
                OwnershipPathStep.fanOut("chronometric-age", "event_fk"),
                OwnershipPathStep.exactlyOne("protocol", "chronometricAgeProtocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment chronometricAgeConversionProtocolReferencesForOccurrence(
      SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "chronometric-age-conversion-protocol-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipPathStep.exactlyOne("event", "event_fk"),
                OwnershipPathStep.fanOut("chronometric-age", "event_fk"),
                OwnershipPathStep.exactlyOne("protocol", "chronometricAgeConversionProtocol_fk"),
                OwnershipPathStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment occurrenceReferences(SchemaGraph graph) {
    return references(
        graph,
        new Spec("occurrence-references", "occurrence-reference", "occurrence_fk", List.of()));
  }

  public static ExtensionFragment materialReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "material-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("material-reference", "materialEntity_fk"))));
  }

  public static ExtensionFragment identificationReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "identification-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipPathStep.fanOut("identification", "occurrence_fk"),
                OwnershipPathStep.fanOut("identification-reference", "identification_fk"))));
  }

  public static ExtensionFragment chronometricAgeReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "chronometric-age-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipPathStep.exactlyOne("event", "event_fk"),
                OwnershipPathStep.fanOut("chronometric-age", "event_fk"),
                OwnershipPathStep.fanOut("chronometric-age-reference", "chronometricAge_fk"))));
  }

  public static ExtensionFragment molecularProtocolReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "material-molecular-protocol-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipPathStep.exactlyOne("molecular-protocol", "molecularProtocol_fk"),
                OwnershipPathStep.fanOut("molecular-protocol-reference", "molecularProtocol_fk"))));
  }

  public static ExtensionFragment dnaAnalysisIdentificationReferencesForOccurrence(
      SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "material-dna-analysis-identification-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipPathStep.fanOut("identification", "nucleotideAnalysis_fk")
                    .filter(FilterExpression.isNull("occurrence_fk")),
                OwnershipPathStep.fanOut("identification-reference", "identification_fk"))));
  }

  public static ExtensionFragment dnaSequenceIdentificationReferencesForOccurrence(
      SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "material-dna-sequence-identification-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipPathStep.exactlyOne("nucleotide-sequence", "nucleotideSequence_fk"),
                OwnershipPathStep.fanOut("identification", "nucleotideSequence_fk")
                    .filter(
                        FilterExpression.and(
                            FilterExpression.isNull("occurrence_fk"),
                            FilterExpression.isNull("nucleotideAnalysis_fk"))),
                OwnershipPathStep.fanOut("identification-reference", "identification_fk"))));
  }

  private static ExtensionFragment references(SchemaGraph graph, Spec spec) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(spec, "spec");

    MappingPath source = MappingPath.root(graph, spec.sourceResource());
    MappingPath current = source;
    for (OwnershipPathStep step : spec.ownershipPath()) {
      current = step.appendTo(current);
    }
    MappingPath reference =
        current.join("bibliographic-resource").via("reference_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment(spec.fragmentName(), ROW_TYPE_REFERENCE, reference)
            .scopeKey(spec.scopeKeyColumn())
            .rowIdentity(reference.field("reference_pk"));
    addReferenceFields(builder, reference);
    if (spec.routeToOccurrence()) {
      OccurrenceExtensionRouting.addOccurrenceId(builder, source);
    }
    return builder.build();
  }

  private static void addReferenceFields(ExtensionFragmentBuilder builder, MappingPath reference) {
    explicit(builder, reference, "referenceID", DcTerm.identifier.qualifiedName());
    explicit(
        builder, reference, "bibliographicCitation", DcTerm.bibliographicCitation.qualifiedName());
    explicit(builder, reference, "title", DcTerm.title.qualifiedName());
    explicit(builder, reference, "author", DcTerm.creator.qualifiedName());
    explicit(builder, reference, "issued", DcTerm.date.qualifiedName());
    explicit(builder, reference, "isPartOfReferenceID", DcTerm.source.qualifiedName());
    explicit(builder, reference, "referenceRemarks", DcTerm.description.qualifiedName());
    explicit(builder, reference, "referenceType", DcTerm.type.qualifiedName());
  }

  private static void explicit(
      ExtensionFragmentBuilder builder, MappingPath source, String sourceField, String targetTerm) {
    builder.field(
        TargetFieldMapping.oneOf(
            targetTerm, ValueAggregation.firstNonNull(), source.field(sourceField)));
  }

  private record Spec(
      String fragmentName,
      String sourceResource,
      String scopeKeyColumn,
      List<OwnershipPathStep> ownershipPath,
      boolean routeToOccurrence) {

    private Spec(
        String fragmentName,
        String sourceResource,
        String scopeKeyColumn,
        List<OwnershipPathStep> ownershipPath) {
      this(fragmentName, sourceResource, scopeKeyColumn, ownershipPath, false);
    }

    static Spec routed(
        String fragmentName,
        String sourceResource,
        String scopeKeyColumn,
        List<OwnershipPathStep> ownershipPath) {
      return new Spec(fragmentName, sourceResource, scopeKeyColumn, ownershipPath, true);
    }

    private Spec {
      Objects.requireNonNull(fragmentName, "fragmentName");
      Objects.requireNonNull(sourceResource, "sourceResource");
      Objects.requireNonNull(scopeKeyColumn, "scopeKeyColumn");
      ownershipPath = List.copyOf(ownershipPath);
    }
  }
}
