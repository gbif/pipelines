package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;
import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression.field;

import java.util.Map;
import java.util.Optional;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.dwc.terms.MixsTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CaseExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.PredicateExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaResource;

/** Declarative mappings for the GBIF DNA Derived Data extension. */
public final class NucleotideMapping {

  public static final String ROW_TYPE_DNA_DERIVED_DATA = Extension.DNA_DERIVED_DATA.getRowType();

  public static final String EVENT_READ_TOTALS = "__dwcdp_event_read_totals";

  // Terms in the registered DNA Derived Data extension that the term factory cannot resolve
  // from the DwC-DP field names.
  private static final Map<String, String> MOLECULAR_PROTOCOL_TERM_OVERRIDES =
      Map.of(
          "samp_taxon_id", "https://w3id.org/mixs/0001320",
          "neg_cont_type", "https://w3id.org/mixs/0001321",
          "pos_cont_type", "https://w3id.org/mixs/0001322",
          "single_cell_lysis_appr", "https://w3id.org/mixs/0000076",
          "single_cell_lysis_prot", "https://w3id.org/mixs/0000054");

  private NucleotideMapping() {}

  /**
   * DNA-analysis rows owned directly by an Event (eDNA/metabarcoding path). Analyses that also
   * carry materialEntity_fk are deliberately excluded: legacy attaches those only through the
   * material/Occurrence path and never duplicates them directly onto Event.
   */
  public static ExtensionFragment eventAnalyses(SchemaGraph graph) {
    EventPaths paths = eventAnalysisPaths(graph);
    ExtensionFragmentBuilder builder =
        extensionFragment("event-nucleotide-analysis", ROW_TYPE_DNA_DERIVED_DATA, paths.analysis())
            .scopeKey("event_pk")
            .rowIdentity(paths.analysis().field("nucleotideAnalysis_pk"));

    DirectFieldMappings.from(graph, "nucleotide-analysis", paths.analysis()).addTo(builder);
    return builder.build();
  }

  /** Nucleotide-sequence fields enriching a directly Event-owned DNA analysis row. */
  public static ExtensionFragment eventAnalysisSequence(SchemaGraph graph) {
    EventPaths paths = eventAnalysisPaths(graph);
    MappingPath sequence =
        paths
            .analysis()
            .join("nucleotide-sequence")
            .via("nucleotideSequence_fk")
            .optional()
            .exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("event-nucleotide-sequence", ROW_TYPE_DNA_DERIVED_DATA, sequence)
            .scopeKey("event_pk")
            .rowMatch(paths.analysis().field("nucleotideAnalysis_pk"));

    addSequenceFields(graph, sequence, builder);
    return builder.build();
  }

  /** Molecular-protocol fields enriching a directly Event-owned DNA analysis row. */
  public static ExtensionFragment eventAnalysisProtocol(SchemaGraph graph) {
    EventPaths paths = eventAnalysisPaths(graph);
    MappingPath protocol =
        paths
            .analysis()
            .join("molecular-protocol")
            .via("molecularProtocol_fk")
            .optional()
            .exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("event-molecular-protocol", ROW_TYPE_DNA_DERIVED_DATA, protocol)
            .scopeKey("event_pk")
            .rowMatch(paths.analysis().field("nucleotideAnalysis_pk"));

    addMolecularProtocolFields(graph, protocol, builder);
    return builder.build();
  }

  /** Sequences and matching analysis and protocol metadata linked through identifications. */
  public static ExtensionFragment identificationSequencesForOccurrence(SchemaGraph graph) {
    return identificationSequences(
        graph,
        MappingPath.root(graph, "occurrence"),
        "identification-nucleotide-sequence-for-occurrence",
        "occurrence_pk");
  }

  /** Collects the distinct processed totals of analyses linked to the Event. */
  public static CoreFragment eventReadTotals(SchemaGraph graph) {
    MappingPath analysis =
        MappingPath.root(graph, "event")
            .join("nucleotide-analysis")
            .via("event_fk")
            .optional()
            .fanOut();
    return CoreFragmentBuilder.coreFragment("event-dna-read-totals", analysis)
        .field(
            TargetFieldMapping.oneOf(
                EVENT_READ_TOTALS,
                ValueAggregation.firstNonNull(),
                analysis.field("processedTotalReadCount")))
        .build();
  }

  /** Sequences associated with an occurrence nested under its Event. */
  public static ExtensionFragment identificationSequencesForEventOccurrence(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");
    return identificationSequences(
        graph,
        event.join("occurrence").via("event_fk").optional().fanOut(),
        "identification-nucleotide-sequence-for-event-occurrence",
        "event_pk");
  }

  private static ExtensionFragment identificationSequences(
      SchemaGraph graph, MappingPath occurrence, String name, String scopeKey) {
    MappingPath identification =
        occurrence.join("identification").via("occurrence_fk").optional().fanOut();
    MappingPath sequence =
        identification
            .join("nucleotide-sequence")
            .via("nucleotideSequence_fk")
            .optional()
            .exactlyOne();

    MappingPath analysis =
        sequence.join("nucleotide-analysis").via("nucleotideSequence_fk").optional().fanOut();
    ValueExpression extract = field(identification.field("materialEntity_fk"));
    MappingPath protocol =
        analysis
            .andOn(
                PredicateExpression.any(
                    PredicateExpression.equals(extract, field(analysis.field("materialEntity_fk"))),
                    PredicateExpression.all(
                        PredicateExpression.isNull(extract),
                        PredicateExpression.equals(
                            field(occurrence.field("event_fk")),
                            field(analysis.field("event_fk"))))))
            .join("molecular-protocol")
            .via("molecularProtocol_fk")
            .optional()
            .exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment(name, ROW_TYPE_DNA_DERIVED_DATA, protocol).scopeKey(scopeKey);
    if (occurrence.rootResource().equals("event")) {
      builder.field(
          TargetFieldMapping.expression(
              DwcTerm.occurrenceID.qualifiedName(),
              CaseExpression.builder()
                  .when(
                      PredicateExpression.isNull(field(sequence.field("nucleotideSequence_pk"))),
                      ValueExpression.literal(null))
                  .otherwise(OccurrenceExtensionRouting.occurrenceId(occurrence))));
    }

    addSequenceFields(graph, sequence, builder);
    DirectFieldMappings.from(graph, "nucleotide-analysis", analysis).addTo(builder);
    addMolecularProtocolFields(graph, protocol, builder);
    return builder.build();
  }

  /**
   * Base DNA-analysis rows belonging to the occurrence's single unambiguous evidence material.
   * Every analysis on that material is an independent extension row.
   */
  public static ExtensionFragment materialAnalysesForOccurrence(SchemaGraph graph) {
    Paths paths = occurrenceAnalysisPaths(graph);
    ExtensionFragmentBuilder builder =
        extensionFragment(
                "material-nucleotide-analysis-for-occurrence",
                ROW_TYPE_DNA_DERIVED_DATA,
                paths.analysis())
            .scopeKey("occurrence_pk")
            .rowIdentity(paths.analysis().field("nucleotideAnalysis_pk"));

    DirectFieldMappings.from(graph, "nucleotide-analysis", paths.analysis()).addTo(builder);
    return builder.build();
  }

  /** Nucleotide-sequence fields enriching the DNA row identified by nucleotideAnalysis_pk. */
  public static ExtensionFragment materialAnalysisSequenceForOccurrence(SchemaGraph graph) {
    Paths paths = occurrenceAnalysisPaths(graph);
    MappingPath sequence =
        paths
            .analysis()
            .join("nucleotide-sequence")
            .via("nucleotideSequence_fk")
            .optional()
            .exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "material-nucleotide-sequence-for-occurrence", ROW_TYPE_DNA_DERIVED_DATA, sequence)
            .scopeKey("occurrence_pk")
            .rowMatch(paths.analysis().field("nucleotideAnalysis_pk"));

    addSequenceFields(graph, sequence, builder);
    return builder.build();
  }

  /** Molecular-protocol fields enriching the DNA row identified by nucleotideAnalysis_pk. */
  public static ExtensionFragment materialAnalysisProtocolForOccurrence(SchemaGraph graph) {
    Paths paths = occurrenceAnalysisPaths(graph);
    MappingPath protocol =
        paths
            .analysis()
            .join("molecular-protocol")
            .via("molecularProtocol_fk")
            .optional()
            .exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "material-molecular-protocol-for-occurrence", ROW_TYPE_DNA_DERIVED_DATA, protocol)
            .scopeKey("occurrence_pk")
            .rowMatch(paths.analysis().field("nucleotideAnalysis_pk"));

    addMolecularProtocolFields(graph, protocol, builder);
    return builder.build();
  }

  private static EventPaths eventAnalysisPaths(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");
    MappingPath analysis =
        event
            .join("nucleotide-analysis")
            .via("event_fk")
            .filter(FilterExpression.isNull("materialEntity_fk"))
            .optional()
            .fanOut();
    return new EventPaths(analysis);
  }

  private static Paths occurrenceAnalysisPaths(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath analysis =
        material.join("nucleotide-analysis").via("materialEntity_fk").optional().fanOut();
    return new Paths(material, analysis);
  }

  private static void addSequenceFields(
      SchemaGraph graph, MappingPath sequence, ExtensionFragmentBuilder builder) {
    SchemaResource resource = requiredResource(graph, "nucleotide-sequence");
    for (String column : resource.fields().keySet()) {
      if (isStructural(column) || column.equals("sequence")) {
        continue;
      }
      TargetTerms.resolveOutput(column)
          .ifPresent(target -> builder.field(inferred(target, sequence.field(column))));
    }
    builder.field(inferred(GbifDnaTerm.dna_sequence.qualifiedName(), sequence.field("sequence")));
  }

  private static void addMolecularProtocolFields(
      SchemaGraph graph, MappingPath protocol, ExtensionFragmentBuilder builder) {
    SchemaResource resource = requiredResource(graph, "molecular-protocol");
    String dnaSequenceTarget = GbifDnaTerm.dna_sequence.qualifiedName();
    for (String column : resource.fields().keySet()) {
      if (isStructural(column) || column.equals("target_gene")) {
        continue;
      }
      Optional.ofNullable(MOLECULAR_PROTOCOL_TERM_OVERRIDES.get(column))
          .or(() -> TargetTerms.resolveOutput(column))
          .filter(target -> !target.equals(dnaSequenceTarget))
          .ifPresent(target -> builder.field(inferred(target, protocol.field(column))));
    }
    builder.field(inferred(MixsTerm.target_gene.qualifiedName(), protocol.field("target_gene")));
  }

  private static SchemaResource requiredResource(SchemaGraph graph, String resource) {
    return graph
        .resource(resource)
        .orElseThrow(
            () -> new IllegalArgumentException("DwC-DP schema has no resource " + resource));
  }

  private static TargetFieldMapping inferred(String target, FieldRef field) {
    return TargetFieldMapping.inferredOneOf(target, ValueAggregation.firstNonNull(), field);
  }

  private static boolean isStructural(String column) {
    return column.endsWith("_pk") || column.endsWith("_fk");
  }

  private record EventPaths(MappingPath analysis) {}

  private record Paths(MappingPath material, MappingPath analysis) {}
}
