package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationRequirement;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;

/** Declarative mappings for DwC Identification extension rows. */
public final class IdentificationMapping {

  /** Extension.IDENTIFICATION.getRowType(). */
  public static final String ROW_TYPE_IDENTIFICATION =
      "http://rs.tdwg.org/dwc/terms/Identification";

  private static final String IDENTIFICATION = "identification";
  private static final String AGENT = "agent";

  private IdentificationMapping() {}

  /**
   * Complete identification history attached directly to an Occurrence. Accepted and historical
   * rows are all preserved; the sole-accepted flattening is a separate core enrichment.
   */
  public static ExtensionFragment occurrenceHistory(SchemaGraph graph) {
    SchemaPath identification = SchemaPath.root(IDENTIFICATION);
    SchemaPath agent =
        identification.append(graph.resolve(IDENTIFICATION, AGENT, "identifiedByID", null));
    ExtensionFragmentBuilder builder =
        extensionFragment(
                "occurrence-identification-history", ROW_TYPE_IDENTIFICATION, IDENTIFICATION)
            .scopeKey("occurrence_fk")
            .join(AGENT)
            .via("identifiedByID")
            .optional()
            .fanOut()
            .rowIdentity(identification.field("identification_pk"));
    addIdentificationFields(graph, builder, identification, agent);
    return builder.build();
  }

  /** Identifications explicitly linked to a directly Event-owned nucleotide analysis. */
  public static ExtensionFragment eventDnaAnalysisIdentifications(SchemaGraph graph) {
    return dnaIdentifications(
        graph,
        new DnaSpec(
            "event-dna-analysis-identifications",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipStep.fanOut(IDENTIFICATION, "nucleotideAnalysis_fk")
                    .filter(FilterExpression.isNull("occurrence_fk")))));
  }

  /**
   * Identifications linked to the sequence of a directly Event-owned analysis. A direct
   * nucleotideAnalysis_fk is stronger and therefore excludes the sequence-derived ownership path.
   */
  public static ExtensionFragment eventDnaSequenceIdentifications(SchemaGraph graph) {
    return dnaIdentifications(
        graph,
        new DnaSpec(
            "event-dna-sequence-identifications",
            "event",
            "event_pk",
            List.of(
                OwnershipStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipStep.exactlyOne("nucleotide-sequence", "nucleotideSequence_fk"),
                OwnershipStep.fanOut(IDENTIFICATION, "nucleotideSequence_fk")
                    .filter(
                        FilterExpression.and(
                            FilterExpression.isNull("occurrence_fk"),
                            FilterExpression.isNull("nucleotideAnalysis_fk"))))));
  }

  /**
   * Identifications explicitly linked to a DNA analysis owned by the Occurrence's evidence
   * Material.
   */
  public static ExtensionFragment occurrenceDnaAnalysisIdentifications(SchemaGraph graph) {
    return dnaIdentifications(
        graph,
        new DnaSpec(
            "material-dna-analysis-identifications-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipStep.fanOut(IDENTIFICATION, "nucleotideAnalysis_fk")
                    .filter(FilterExpression.isNull("occurrence_fk")))));
  }

  /**
   * Identifications linked through a nucleotide sequence owned by the Occurrence's evidence
   * Material. Explicit occurrence_fk and nucleotideAnalysis_fk ownership take precedence.
   */
  public static ExtensionFragment occurrenceDnaSequenceIdentifications(SchemaGraph graph) {
    return dnaIdentifications(
        graph,
        new DnaSpec(
            "material-dna-sequence-identifications-for-occurrence",
            "occurrence",
            "occurrence_pk",
            List.of(
                OwnershipStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipStep.exactlyOne("nucleotide-sequence", "nucleotideSequence_fk"),
                OwnershipStep.fanOut(IDENTIFICATION, "nucleotideSequence_fk")
                    .filter(
                        FilterExpression.and(
                            FilterExpression.isNull("occurrence_fk"),
                            FilterExpression.isNull("nucleotideAnalysis_fk"))))));
  }

  private static ExtensionFragment dnaIdentifications(SchemaGraph graph, DnaSpec spec) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(spec, "spec");

    SchemaPath current = SchemaPath.root(spec.sourceResource());
    ExtensionFragmentBuilder builder =
        extensionFragment(spec.fragmentName(), ROW_TYPE_IDENTIFICATION, spec.sourceResource())
            .scopeKey(spec.scopeKeyColumn());

    for (OwnershipStep step : spec.ownershipPath()) {
      current =
          current.append(
              graph.resolve(current.currentResource(), step.resource(), step.viaColumn(), null));
      addOwnershipStep(builder, step);
    }

    if (!current.currentResource().equals(IDENTIFICATION)) {
      throw new IllegalArgumentException(
          "DNA identification ownership path must end at " + IDENTIFICATION);
    }

    SchemaPath identification = current;
    SchemaPath agent =
        identification.append(graph.resolve(IDENTIFICATION, AGENT, "identifiedByID", null));

    builder
        .join(AGENT)
        .via("identifiedByID")
        .optional()
        .fanOut()
        .rowIdentity(identification.field("identification_pk"));

    addIdentificationFields(graph, builder, identification, agent);
    return builder.build();
  }

  private static void addIdentificationFields(
      SchemaGraph graph,
      ExtensionFragmentBuilder builder,
      SchemaPath identification,
      SchemaPath agent) {
    DirectFieldMappings.from(graph, IDENTIFICATION, identification).addTo(builder);
    builder.field(
        TargetFieldMapping.oneOf(
            DwcTerm.identifiedBy.qualifiedName(),
            ValueAggregation.firstNonNull(),
            identification.field("identifiedBy"),
            agent.field("preferredAgentName")));
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

  private record DnaSpec(
      String fragmentName,
      String sourceResource,
      String scopeKeyColumn,
      List<OwnershipStep> ownershipPath) {
    private DnaSpec {
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
      return new OwnershipStep(
          resource, viaColumn, false, RelationRequirement.OPTIONAL, Optional.empty());
    }

    static OwnershipStep exactlyOne(String resource, String viaColumn) {
      return new OwnershipStep(
          resource, viaColumn, true, RelationRequirement.OPTIONAL, Optional.empty());
    }

    OwnershipStep filter(FilterExpression expression) {
      return new OwnershipStep(
          resource, viaColumn, exactlyOne, requirement, Optional.of(expression));
    }
  }
}
