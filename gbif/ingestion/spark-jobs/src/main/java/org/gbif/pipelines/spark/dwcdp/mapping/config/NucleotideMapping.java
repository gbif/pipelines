package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder.extensionFragment;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.dwc.terms.MixsTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaResource;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

/** Declarative mappings for the GBIF DNA Derived Data extension. */
public final class NucleotideMapping {

  public static final String ROW_TYPE_DNA_DERIVED_DATA = Extension.DNA_DERIVED_DATA.getRowType();

  private NucleotideMapping() {}


  /**
   * DNA-analysis rows owned directly by an Event (eDNA/metabarcoding path). Analyses that also
   * carry materialEntity_fk are deliberately excluded: legacy attaches those only through the
   * material/Occurrence path and never duplicates them directly onto Event.
   */
  public static ExtensionFragment eventAnalyses(SchemaGraph graph) {
    EventPaths paths = eventAnalysisPaths(graph);
    ExtensionFragmentBuilder builder =
        extensionFragment(
                "event-nucleotide-analysis",
                ROW_TYPE_DNA_DERIVED_DATA,
                "event")
            .scopeKey("event_pk")
            .join("nucleotide-analysis")
            .via("event_fk")
            .filter(cols -> cols.col("materialEntity_fk").isNull())
            .optional()
            .fanOut()
            .rowIdentity(paths.analysis().field("nucleotideAnalysis_pk"));

    DirectFieldMappings.from(graph, "nucleotide-analysis", paths.analysis()).addTo(builder);
    return builder.build();
  }

  /** Nucleotide-sequence fields enriching a directly Event-owned DNA analysis row. */
  public static ExtensionFragment eventAnalysisSequence(SchemaGraph graph) {
    EventPaths paths = eventAnalysisPaths(graph);
    SchemaPath sequence =
        paths.analysis().append(
            graph.resolve(
                "nucleotide-analysis", "nucleotide-sequence", "nucleotideSequence_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "event-nucleotide-sequence",
                ROW_TYPE_DNA_DERIVED_DATA,
                "event")
            .scopeKey("event_pk")
            .join("nucleotide-analysis")
            .via("event_fk")
            .filter(cols -> cols.col("materialEntity_fk").isNull())
            .optional()
            .fanOut()
            .join("nucleotide-sequence")
            .via("nucleotideSequence_fk")
            .optional()
            .exactlyOne()
            .rowMatch(paths.analysis().field("nucleotideAnalysis_pk"));

    addSequenceFields(graph, sequence, builder);
    return builder.build();
  }

  /** Molecular-protocol fields enriching a directly Event-owned DNA analysis row. */
  public static ExtensionFragment eventAnalysisProtocol(SchemaGraph graph) {
    EventPaths paths = eventAnalysisPaths(graph);
    SchemaPath protocol =
        paths.analysis().append(
            graph.resolve(
                "nucleotide-analysis", "molecular-protocol", "molecularProtocol_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "event-molecular-protocol",
                ROW_TYPE_DNA_DERIVED_DATA,
                "event")
            .scopeKey("event_pk")
            .join("nucleotide-analysis")
            .via("event_fk")
            .filter(cols -> cols.col("materialEntity_fk").isNull())
            .optional()
            .fanOut()
            .join("molecular-protocol")
            .via("molecularProtocol_fk")
            .optional()
            .exactlyOne()
            .rowMatch(paths.analysis().field("nucleotideAnalysis_pk"));

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
                "occurrence")
            .scopeKey("occurrence_pk")
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("nucleotide-analysis")
            .via("materialEntity_fk")
            .optional()
            .fanOut()
            .rowIdentity(paths.analysis().field("nucleotideAnalysis_pk"));

    DirectFieldMappings.from(graph, "nucleotide-analysis", paths.analysis()).addTo(builder);
    return builder.build();
  }

  /** Nucleotide-sequence fields enriching the DNA row identified by nucleotideAnalysis_pk. */
  public static ExtensionFragment materialAnalysisSequenceForOccurrence(SchemaGraph graph) {
    Paths paths = occurrenceAnalysisPaths(graph);
    SchemaPath sequence =
        paths.analysis().append(
            graph.resolve(
                "nucleotide-analysis", "nucleotide-sequence", "nucleotideSequence_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "material-nucleotide-sequence-for-occurrence",
                ROW_TYPE_DNA_DERIVED_DATA,
                "occurrence")
            .scopeKey("occurrence_pk")
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("nucleotide-analysis")
            .via("materialEntity_fk")
            .optional()
            .fanOut()
            .join("nucleotide-sequence")
            .via("nucleotideSequence_fk")
            .optional()
            .exactlyOne()
            .rowMatch(paths.analysis().field("nucleotideAnalysis_pk"));

    addSequenceFields(graph, sequence, builder);
    return builder.build();
  }

  /** Molecular-protocol fields enriching the DNA row identified by nucleotideAnalysis_pk. */
  public static ExtensionFragment materialAnalysisProtocolForOccurrence(SchemaGraph graph) {
    Paths paths = occurrenceAnalysisPaths(graph);
    SchemaPath protocol =
        paths.analysis().append(
            graph.resolve(
                "nucleotide-analysis", "molecular-protocol", "molecularProtocol_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "material-molecular-protocol-for-occurrence",
                ROW_TYPE_DNA_DERIVED_DATA,
                "occurrence")
            .scopeKey("occurrence_pk")
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("nucleotide-analysis")
            .via("materialEntity_fk")
            .optional()
            .fanOut()
            .join("molecular-protocol")
            .via("molecularProtocol_fk")
            .optional()
            .exactlyOne()
            .rowMatch(paths.analysis().field("nucleotideAnalysis_pk"));

    addMolecularProtocolFields(graph, protocol, builder);
    return builder.build();
  }


  private static EventPaths eventAnalysisPaths(SchemaGraph graph) {
    SchemaPath event = SchemaPath.root("event");
    SchemaPath analysis =
        event.append(graph.resolve("event", "nucleotide-analysis", "event_fk", null));
    return new EventPaths(analysis);
  }

  private static Paths occurrenceAnalysisPaths(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID", null));
    SchemaPath analysis =
        material.append(graph.resolve("material", "nucleotide-analysis", "materialEntity_fk", null));
    return new Paths(material, analysis);
  }

  private static void addSequenceFields(
      SchemaGraph graph, SchemaPath sequence, ExtensionFragmentBuilder builder) {
    SchemaResource resource = requiredResource(graph, "nucleotide-sequence");
    for (String column : resource.fields().keySet()) {
      if (isStructural(column) || column.equals("sequence")) {
        continue;
      }
      builder.field(inferred(TargetTerms.resolve(column), sequence.field(column)));
    }
    builder.field(inferred(GbifDnaTerm.dna_sequence.qualifiedName(), sequence.field("sequence")));
  }

  private static void addMolecularProtocolFields(
      SchemaGraph graph, SchemaPath protocol, ExtensionFragmentBuilder builder) {
    SchemaResource resource = requiredResource(graph, "molecular-protocol");
    String dnaSequenceTarget = GbifDnaTerm.dna_sequence.qualifiedName();
    for (String column : resource.fields().keySet()) {
      String target = TargetTerms.resolve(column);
      if (isStructural(column)
          || column.equals("target_gene")
          || target.equals(dnaSequenceTarget)) {
        continue;
      }
      builder.field(inferred(target, protocol.field(column)));
    }
    builder.field(inferred(MixsTerm.target_gene.qualifiedName(), protocol.field("target_gene")));
  }

  private static SchemaResource requiredResource(SchemaGraph graph, String resource) {
    return graph.resource(resource)
        .orElseThrow(
            () -> new IllegalArgumentException("DwC-DP schema has no resource " + resource));
  }

  private static TargetFieldMapping inferred(String target, FieldRef field) {
    return TargetFieldMapping.inferredOneOf(target, ValueAggregation.firstNonNull(), field);
  }

  private static boolean isStructural(String column) {
    return column.endsWith("_pk") || column.endsWith("_fk");
  }

  private record EventPaths(SchemaPath analysis) {}

  private record Paths(SchemaPath material, SchemaPath analysis) {}
}
