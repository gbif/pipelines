package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationRequirement;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

/** Declarative mappings for the Extended Measurement or Fact (eMoF) extension. */
public final class AssertionMapping {

  public static final String ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT =
      "http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact";

  private AssertionMapping() {}

  /** Direct Event assertions. */
  public static ExtensionFragment eventAssertions(SchemaGraph graph) {
    return assertions(
        graph,
        new Spec(
            "event-assertions",
            "event-assertion",
            "event_fk",
            "event-assertion",
            "event_fk",
            List.of()));
  }

  /** Direct Occurrence assertions. */
  public static ExtensionFragment occurrenceAssertions(SchemaGraph graph) {
    return assertions(
        graph,
        new Spec(
            "occurrence-assertions",
            "occurrence-assertion",
            "occurrence_fk",
            "occurrence-assertion",
            "occurrence_fk",
            List.of()));
  }

  /** Survey assertions promoted to their owning Event. */
  public static ExtensionFragment surveyAssertionsForEvent(SchemaGraph graph) {
    return assertions(
        graph,
        new Spec(
            "survey-assertions-for-event",
            "event",
            "event_pk",
            "survey-assertion",
            "survey_fk",
            List.of(OwnershipStep.fanOut("survey", "event_fk"))));
  }

  /** Assertions attached directly to Event-owned nucleotide analyses. */
  public static ExtensionFragment nucleotideAnalysisAssertionsForEvent(SchemaGraph graph) {
    return assertions(
        graph,
        new Spec(
            "event-nucleotide-analysis-assertions",
            "event",
            "event_pk",
            "nucleotide-analysis-assertion",
            "nucleotideAnalysis_fk",
            List.of(
                OwnershipStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")))));
  }

  /** Assertions attached to the molecular protocol of an Event-owned nucleotide analysis. */
  public static ExtensionFragment molecularProtocolAssertionsForEvent(SchemaGraph graph) {
    return assertions(
        graph,
        new Spec(
            "event-molecular-protocol-assertions",
            "event",
            "event_pk",
            "molecular-protocol-assertion",
            "molecularProtocol_fk",
            List.of(
                OwnershipStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipStep.exactlyOne("molecular-protocol", "molecularProtocol_fk"))));
  }

  /** Chronometric-age assertions promoted to their owning Event. */
  public static ExtensionFragment chronometricAgeAssertionsForEvent(SchemaGraph graph) {
    return assertions(
        graph,
        new Spec(
            "chronometric-age-assertions-for-event",
            "event",
            "event_pk",
            "chronometric-age-assertion",
            "chronometricAge_fk",
            List.of(OwnershipStep.fanOut("chronometric-age", "event_fk"))));
  }

  /** Chronometric-age assertions promoted from the Occurrence's owning Event. */
  public static ExtensionFragment chronometricAgeAssertionsForOccurrence(SchemaGraph graph) {
    return assertions(
        graph,
        new Spec(
            "chronometric-age-assertions-for-occurrence",
            "occurrence",
            "occurrence_pk",
            "chronometric-age-assertion",
            "chronometricAge_fk",
            List.of(
                OwnershipStep.exactlyOne("event", "event_fk"),
                OwnershipStep.fanOut("chronometric-age", "event_fk"))));
  }

  /** Material assertions promoted to an Occurrence through its single evidence material. */
  public static ExtensionFragment materialAssertionsForOccurrence(SchemaGraph graph) {
    return assertions(
        graph,
        new Spec(
            "material-assertions-for-occurrence",
            "occurrence",
            "occurrence_pk",
            "material-assertion",
            "materialEntity_fk",
            List.of(OwnershipStep.exactlyOne("material", "evidenceForOccurrenceID"))));
  }

  /** Nucleotide-analysis assertions promoted through the Occurrence's evidence material. */
  public static ExtensionFragment nucleotideAnalysisAssertionsForOccurrence(SchemaGraph graph) {
    return assertions(
        graph,
        new Spec(
            "material-nucleotide-analysis-assertions-for-occurrence",
            "occurrence",
            "occurrence_pk",
            "nucleotide-analysis-assertion",
            "nucleotideAnalysis_fk",
            List.of(
                OwnershipStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipStep.fanOut("nucleotide-analysis", "materialEntity_fk"))));
  }

  /** Molecular-protocol assertions promoted through the Occurrence's evidence material analyses. */
  public static ExtensionFragment molecularProtocolAssertionsForOccurrence(SchemaGraph graph) {
    return assertions(
        graph,
        new Spec(
            "material-molecular-protocol-assertions-for-occurrence",
            "occurrence",
            "occurrence_pk",
            "molecular-protocol-assertion",
            "molecularProtocol_fk",
            List.of(
                OwnershipStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipStep.exactlyOne("molecular-protocol", "molecularProtocol_fk"))));
  }

  private static ExtensionFragment assertions(SchemaGraph graph, Spec spec) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(spec, "spec");

    MappingPath current = MappingPath.root(graph, spec.sourceResource());
    for (OwnershipStep step : spec.ownershipPath()) {
      current = append(current, step);
    }

    MappingPath assertion =
        current.currentResource().equals(spec.assertionResource())
            ? current
            : current
                .join(spec.assertionResource())
                .via(spec.assertionViaColumn())
                .optional()
                .fanOut();
    MappingPath protocol =
        assertion.join("protocol").via("assertionProtocol_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment(spec.fragmentName(), ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT, protocol)
            .scopeKey(spec.scopeKeyColumn())
            .rowIdentity(assertion.field("assertionID"));
    addAssertionFields(builder, assertion, protocol);
    return builder.build();
  }

  private static MappingPath append(MappingPath current, OwnershipStep step) {
    MappingPath.JoinBuilder relation = current.join(step.resource()).via(step.viaColumn());
    step.filter().ifPresent(relation::filter);
    if (step.requirement() == RelationRequirement.OPTIONAL) {
      relation.optional();
    } else {
      relation.required();
    }
    return step.cardinality() == OwnershipCardinality.EXACTLY_ONE
        ? relation.exactlyOne()
        : relation.fanOut();
  }

  private static void addAssertionFields(
      ExtensionFragmentBuilder builder, MappingPath assertion, MappingPath protocol) {
    explicit(builder, assertion, "assertionID", "measurementID");
    explicit(builder, assertion, "assertionType", "measurementType");
    explicit(builder, assertion, "assertionTypeIRI", "measurementTypeID");
    explicit(builder, assertion, "assertionValue", "measurementValue");
    explicit(builder, assertion, "assertionValueIRI", "measurementValueID");
    explicit(builder, assertion, "assertionUnit", "measurementUnit");
    explicit(builder, assertion, "assertionUnitIRI", "measurementUnitID");
    explicit(builder, assertion, "assertionError", "measurementAccuracy");
    explicit(builder, assertion, "assertionBy", "measurementDeterminedBy");
    explicit(builder, assertion, "assertionMadeDate", "measurementDeterminedDate");
    explicit(builder, assertion, "assertionRemarks", "measurementRemarks");

    builder.field(
        TargetFieldMapping.oneOf(
            TargetTerms.resolve("measurementMethod"),
            ValueAggregation.firstNonNull(),
            protocol.field("protocolDescription")));
  }

  private static void explicit(
      ExtensionFragmentBuilder builder,
      MappingPath source,
      String sourceField,
      String targetField) {
    builder.field(
        TargetFieldMapping.oneOf(
            TargetTerms.resolve(targetField),
            ValueAggregation.firstNonNull(),
            source.field(sourceField)));
  }

  private record Spec(
      String fragmentName,
      String sourceResource,
      String scopeKeyColumn,
      String assertionResource,
      String assertionViaColumn,
      List<OwnershipStep> ownershipPath) {

    private Spec {
      Objects.requireNonNull(fragmentName, "fragmentName");
      Objects.requireNonNull(sourceResource, "sourceResource");
      Objects.requireNonNull(scopeKeyColumn, "scopeKeyColumn");
      Objects.requireNonNull(assertionResource, "assertionResource");
      Objects.requireNonNull(assertionViaColumn, "assertionViaColumn");
      ownershipPath = List.copyOf(ownershipPath);
    }
  }

  private record OwnershipStep(
      String resource,
      String viaColumn,
      RelationRequirement requirement,
      OwnershipCardinality cardinality,
      Optional<FilterExpression> filter) {

    private OwnershipStep {
      Objects.requireNonNull(resource, "resource");
      Objects.requireNonNull(viaColumn, "viaColumn");
      Objects.requireNonNull(requirement, "requirement");
      Objects.requireNonNull(cardinality, "cardinality");
      Objects.requireNonNull(filter, "filter");
    }

    private static OwnershipStep fanOut(String resource, String viaColumn) {
      return new OwnershipStep(
          resource,
          viaColumn,
          RelationRequirement.OPTIONAL,
          OwnershipCardinality.FAN_OUT,
          Optional.empty());
    }

    private static OwnershipStep exactlyOne(String resource, String viaColumn) {
      return new OwnershipStep(
          resource,
          viaColumn,
          RelationRequirement.OPTIONAL,
          OwnershipCardinality.EXACTLY_ONE,
          Optional.empty());
    }

    private OwnershipStep filter(FilterExpression expression) {
      return new OwnershipStep(
          resource, viaColumn, requirement, cardinality, Optional.of(expression));
    }
  }

  private enum OwnershipCardinality {
    FAN_OUT,
    EXACTLY_ONE
  }
}
