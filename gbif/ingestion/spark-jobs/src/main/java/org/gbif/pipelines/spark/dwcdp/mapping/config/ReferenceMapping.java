package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationRequirement;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;

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
                OwnershipStep.exactlyOne("protocol", "eventProtocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment eventGeoreferenceProtocolReferences(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-georeference-protocol-references",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.exactlyOne("protocol", "georeferenceProtocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment eventProtocolReferences(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-protocol-references",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("event-protocol", "event_fk"),
                OwnershipStep.exactlyOne("protocol", "protocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment surveySamplingProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "survey-sampling-protocol-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("survey", "event_fk"),
                OwnershipStep.exactlyOne("protocol", "samplingProtocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment surveySamplingEffortProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "survey-sampling-effort-protocol-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("survey", "event_fk"),
                OwnershipStep.exactlyOne("protocol", "samplingEffortProtocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment surveyProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "survey-protocol-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("survey", "event_fk"),
                OwnershipStep.fanOut("survey-protocol", "survey_fk"),
                OwnershipStep.exactlyOne("protocol", "protocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment chronometricAgeProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "chronometric-age-protocol-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("chronometric-age", "event_fk"),
                OwnershipStep.exactlyOne("protocol", "chronometricAgeProtocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
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
                OwnershipStep.fanOut("chronometric-age", "event_fk"),
                OwnershipStep.exactlyOne("protocol", "chronometricAgeConversionProtocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment surveyReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "survey-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("survey", "event_fk"),
                OwnershipStep.fanOut("survey-reference", "survey_fk"))));
  }

  public static ExtensionFragment chronometricAgeReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "chronometric-age-references-for-event",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("chronometric-age", "event_fk"),
                OwnershipStep.fanOut("chronometric-age-reference", "chronometricAge_fk"))));
  }

  public static ExtensionFragment molecularProtocolReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-molecular-protocol-references",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipStep.exactlyOne("molecular-protocol", "molecularProtocol_fk"),
                OwnershipStep.fanOut("molecular-protocol-reference", "molecularProtocol_fk"))));
  }

  public static ExtensionFragment dnaAnalysisIdentificationReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-dna-analysis-identification-references",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipStep.fanOut("identification", "nucleotideAnalysis_fk")
                    .filter(FilterExpression.isNull("occurrence_fk")),
                OwnershipStep.fanOut("identification-reference", "identification_fk"))));
  }

  public static ExtensionFragment dnaSequenceIdentificationReferencesForEvent(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "event-dna-sequence-identification-references",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipStep.exactlyOne("nucleotide-sequence", "nucleotideSequence_fk"),
                OwnershipStep.fanOut("identification", "nucleotideSequence_fk")
                    .filter(
                        FilterExpression.and(
                            FilterExpression.isNull("occurrence_fk"),
                            FilterExpression.isNull("nucleotideAnalysis_fk"))),
                OwnershipStep.fanOut("identification-reference", "identification_fk"))));
  }

  public static ExtensionFragment occurrenceProtocolReferences(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "occurrence-protocol-references",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipStep.exactlyOne("protocol", "occurrenceProtocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment materialProtocolReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "material-protocol-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipStep.fanOut("material-protocol", "materialEntity_fk"),
                OwnershipStep.exactlyOne("protocol", "protocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment chronometricAgeProtocolReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "chronometric-age-protocol-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipStep.exactlyOne("event", "event_fk"),
                OwnershipStep.fanOut("chronometric-age", "event_fk"),
                OwnershipStep.exactlyOne("protocol", "chronometricAgeProtocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
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
                OwnershipStep.exactlyOne("event", "event_fk"),
                OwnershipStep.fanOut("chronometric-age", "event_fk"),
                OwnershipStep.exactlyOne("protocol", "chronometricAgeConversionProtocol_fk"),
                OwnershipStep.fanOut("protocol-reference", "protocol_fk"))));
  }

  public static ExtensionFragment occurrenceReferences(SchemaGraph graph) {
    return references(
        graph, new Spec("occurrence-references", "occurrence-reference", "occurrence_fk", List.of()));
  }

  public static ExtensionFragment materialReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "material-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipStep.fanOut("material-reference", "materialEntity_fk"))));
  }

  public static ExtensionFragment identificationReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "identification-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipStep.fanOut("identification", "occurrence_fk"),
                OwnershipStep.fanOut("identification-reference", "identification_fk"))));
  }

  public static ExtensionFragment chronometricAgeReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "chronometric-age-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipStep.exactlyOne("event", "event_fk"),
                OwnershipStep.fanOut("chronometric-age", "event_fk"),
                OwnershipStep.fanOut("chronometric-age-reference", "chronometricAge_fk"))));
  }

  public static ExtensionFragment molecularProtocolReferencesForOccurrence(SchemaGraph graph) {
    return references(
        graph,
        new Spec(
            "material-molecular-protocol-references-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipStep.exactlyOne("molecular-protocol", "molecularProtocol_fk"),
                OwnershipStep.fanOut("molecular-protocol-reference", "molecularProtocol_fk"))));
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
                OwnershipStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipStep.fanOut("identification", "nucleotideAnalysis_fk")
                    .filter(FilterExpression.isNull("occurrence_fk")),
                OwnershipStep.fanOut("identification-reference", "identification_fk"))));
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
                OwnershipStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipStep.exactlyOne("nucleotide-sequence", "nucleotideSequence_fk"),
                OwnershipStep.fanOut("identification", "nucleotideSequence_fk")
                    .filter(
                        FilterExpression.and(
                            FilterExpression.isNull("occurrence_fk"),
                            FilterExpression.isNull("nucleotideAnalysis_fk"))),
                OwnershipStep.fanOut("identification-reference", "identification_fk"))));
  }

  private static ExtensionFragment references(SchemaGraph graph, Spec spec) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(spec, "spec");

    SchemaPath current = SchemaPath.root(spec.sourceResource());
    ExtensionFragmentBuilder builder =
        extensionFragment(spec.fragmentName(), ROW_TYPE_REFERENCE, spec.sourceResource())
            .scopeKey(spec.scopeKeyColumn());

    for (OwnershipStep step : spec.ownershipPath()) {
      current =
          current.append(
              graph.resolve(current.currentResource(), step.resource(), step.viaColumn(), null));
      addOwnershipStep(builder, step);
    }

    SchemaPath reference =
        current.append(
            graph.resolve(current.currentResource(), "bibliographic-resource", "reference_fk", null));
    builder
        .join("bibliographic-resource")
        .via("reference_fk")
        .optional()
        .exactlyOne()
        .rowIdentity(reference.field("reference_pk"));

    addReferenceFields(builder, reference);
    return builder.build();
  }

  private static void addOwnershipStep(ExtensionFragmentBuilder builder, OwnershipStep step) {
    ExtensionFragmentBuilder.RelationBuilder relation =
        builder.join(step.resource()).via(step.viaColumn());
    step.filter().ifPresent(relation::filter);
    if (step.requirement() == RelationRequirement.OPTIONAL) {
      relation.optional();
    } else {
      relation.required();
    }
    if (step.exactlyOne()) {
      relation.exactlyOne();
    } else {
      relation.fanOut();
    }
    relation.endJoin();
  }

  private static void addReferenceFields(ExtensionFragmentBuilder builder, SchemaPath reference) {
    explicit(builder, reference, "referenceID", DcTerm.identifier.qualifiedName());
    explicit(builder, reference, "bibliographicCitation", DcTerm.bibliographicCitation.qualifiedName());
    explicit(builder, reference, "title", DcTerm.title.qualifiedName());
    explicit(builder, reference, "author", DcTerm.creator.qualifiedName());
    explicit(builder, reference, "issued", DcTerm.date.qualifiedName());
    explicit(builder, reference, "isPartOfReferenceID", DcTerm.source.qualifiedName());
    explicit(builder, reference, "referenceRemarks", DcTerm.description.qualifiedName());
    explicit(builder, reference, "referenceType", DcTerm.type.qualifiedName());
  }

  private static void explicit(
      ExtensionFragmentBuilder builder, SchemaPath source, String sourceField, String targetTerm) {
    builder.field(TargetFieldMapping.oneOf(
        targetTerm, ValueAggregation.firstNonNull(), source.field(sourceField)));
  }

  private record Spec(
      String fragmentName,
      String sourceResource,
      String scopeKeyColumn,
      List<OwnershipStep> ownershipPath) {
    private Spec {
      Objects.requireNonNull(fragmentName, "fragmentName");
      Objects.requireNonNull(sourceResource, "sourceResource");
      Objects.requireNonNull(scopeKeyColumn, "scopeKeyColumn");
      ownershipPath = List.copyOf(ownershipPath);
    }
  }

  private record OwnershipStep(
      String resource,
      String viaColumn,
      boolean exactlyOne,
      RelationRequirement requirement,
      Optional<FilterExpression> filter) {
    private OwnershipStep {
      Objects.requireNonNull(resource, "resource");
      Objects.requireNonNull(viaColumn, "viaColumn");
      Objects.requireNonNull(requirement, "requirement");
      Objects.requireNonNull(filter, "filter");
    }

    static OwnershipStep fanOut(String resource, String viaColumn) {
      return new OwnershipStep(resource, viaColumn, false, RelationRequirement.OPTIONAL, Optional.empty());
    }

    static OwnershipStep exactlyOne(String resource, String viaColumn) {
      return new OwnershipStep(resource, viaColumn, true, RelationRequirement.OPTIONAL, Optional.empty());
    }

    OwnershipStep filter(FilterExpression expression) {
      return new OwnershipStep(resource, viaColumn, exactlyOne, requirement, Optional.of(expression));
    }
  }
}
