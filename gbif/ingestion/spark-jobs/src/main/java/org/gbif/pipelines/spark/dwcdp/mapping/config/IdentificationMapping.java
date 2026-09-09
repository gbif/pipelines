package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.List;
import java.util.Objects;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

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
    MappingPath identification = MappingPath.root(graph, IDENTIFICATION);
    MappingPath agent = identification.join(AGENT).via("identifiedByID").optional().fanOut();
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-identification-history", ROW_TYPE_IDENTIFICATION, agent)
            .scopeKey("occurrence_fk")
            .rowIdentity(identification.field("identification_pk"));
    addIdentificationFields(graph, builder, identification, agent);
    return builder.build();
  }

  /** Occurrence identification history promoted to Event core and routed to that occurrence. */
  public static ExtensionFragment occurrenceHistoryForEvent(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification =
        occurrence.join(IDENTIFICATION).via("occurrence_fk").optional().fanOut();
    MappingPath agent = identification.join(AGENT).via("identifiedByID").optional().fanOut();
    ExtensionFragmentBuilder builder =
        extensionFragment(
                "occurrence-identification-history-for-event", ROW_TYPE_IDENTIFICATION, agent)
            .scopeKey("event_fk")
            .rowIdentity(identification.field("identification_pk"));
    addIdentificationFields(graph, builder, identification, agent);
    OccurrenceExtensionRouting.addOccurrenceId(builder, occurrence);
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
                OwnershipPathStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipPathStep.fanOut(IDENTIFICATION, "nucleotideAnalysis_fk")
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
                OwnershipPathStep.fanOut("nucleotide-analysis", "event_fk")
                    .filter(FilterExpression.isNull("materialEntity_fk")),
                OwnershipPathStep.exactlyOne("nucleotide-sequence", "nucleotideSequence_fk"),
                OwnershipPathStep.fanOut(IDENTIFICATION, "nucleotideSequence_fk")
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
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipPathStep.fanOut(IDENTIFICATION, "nucleotideAnalysis_fk")
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
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipPathStep.exactlyOne("nucleotide-sequence", "nucleotideSequence_fk"),
                OwnershipPathStep.fanOut(IDENTIFICATION, "nucleotideSequence_fk")
                    .filter(
                        FilterExpression.and(
                            FilterExpression.isNull("occurrence_fk"),
                            FilterExpression.isNull("nucleotideAnalysis_fk"))))));
  }

  /** DNA-analysis identifications owned through a specific Event-nested Occurrence. */
  public static ExtensionFragment occurrenceDnaAnalysisIdentificationsForEvent(SchemaGraph graph) {
    return dnaIdentifications(
        graph,
        DnaSpec.routed(
            "material-dna-analysis-identifications-for-event-occurrence",
            "occurrence",
            "event_fk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipPathStep.fanOut(IDENTIFICATION, "nucleotideAnalysis_fk")
                    .filter(FilterExpression.isNull("occurrence_fk")))));
  }

  /** DNA-sequence identifications owned through a specific Event-nested Occurrence. */
  public static ExtensionFragment occurrenceDnaSequenceIdentificationsForEvent(SchemaGraph graph) {
    return dnaIdentifications(
        graph,
        DnaSpec.routed(
            "material-dna-sequence-identifications-for-event-occurrence",
            "occurrence",
            "event_fk",
            List.of(
                OwnershipPathStep.exactlyOne("material", "evidenceForOccurrenceID"),
                OwnershipPathStep.fanOut("nucleotide-analysis", "materialEntity_fk"),
                OwnershipPathStep.exactlyOne("nucleotide-sequence", "nucleotideSequence_fk"),
                OwnershipPathStep.fanOut(IDENTIFICATION, "nucleotideSequence_fk")
                    .filter(
                        FilterExpression.and(
                            FilterExpression.isNull("occurrence_fk"),
                            FilterExpression.isNull("nucleotideAnalysis_fk"))))));
  }

  private static ExtensionFragment dnaIdentifications(SchemaGraph graph, DnaSpec spec) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(spec, "spec");

    MappingPath source = MappingPath.root(graph, spec.sourceResource());
    MappingPath current = source;
    for (OwnershipPathStep step : spec.ownershipPath()) {
      current = step.appendTo(current);
    }
    if (!current.currentResource().equals(IDENTIFICATION)) {
      throw new IllegalArgumentException(
          "DNA identification ownership path must end at " + IDENTIFICATION);
    }

    MappingPath identification = current;
    MappingPath agent = identification.join(AGENT).via("identifiedByID").optional().fanOut();
    ExtensionFragmentBuilder builder =
        extensionFragment(spec.fragmentName(), ROW_TYPE_IDENTIFICATION, agent)
            .scopeKey(spec.scopeKeyColumn())
            .rowIdentity(identification.field("identification_pk"));

    addIdentificationFields(graph, builder, identification, agent);
    if (spec.routeToOccurrence()) {
      OccurrenceExtensionRouting.addOccurrenceId(builder, source);
    }
    return builder.build();
  }

  private static void addIdentificationFields(
      SchemaGraph graph,
      ExtensionFragmentBuilder builder,
      MappingPath identification,
      MappingPath agent) {
    DirectFieldMappings.from(graph, IDENTIFICATION, identification).addTo(builder);
    builder.field(
        TargetFieldMapping.oneOf(
            DwcTerm.identifiedBy.qualifiedName(),
            ValueAggregation.firstNonNull(),
            identification.field("identifiedBy"),
            agent.field("preferredAgentName")));
  }

  private record DnaSpec(
      String fragmentName,
      String sourceResource,
      String scopeKeyColumn,
      List<OwnershipPathStep> ownershipPath,
      boolean routeToOccurrence) {

    private DnaSpec(
        String fragmentName,
        String sourceResource,
        String scopeKeyColumn,
        List<OwnershipPathStep> ownershipPath) {
      this(fragmentName, sourceResource, scopeKeyColumn, ownershipPath, false);
    }

    static DnaSpec routed(
        String fragmentName,
        String sourceResource,
        String scopeKeyColumn,
        List<OwnershipPathStep> ownershipPath) {
      return new DnaSpec(fragmentName, sourceResource, scopeKeyColumn, ownershipPath, true);
    }

    private DnaSpec {
      Objects.requireNonNull(fragmentName, "fragmentName");
      Objects.requireNonNull(sourceResource, "sourceResource");
      Objects.requireNonNull(scopeKeyColumn, "scopeKeyColumn");
      ownershipPath = List.copyOf(ownershipPath);
    }
  }
}
