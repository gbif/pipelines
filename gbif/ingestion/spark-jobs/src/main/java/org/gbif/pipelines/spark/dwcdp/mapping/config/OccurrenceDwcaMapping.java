package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder.mappingPlan;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;

/** Occurrence-core mapping configurations assembled during parity migration. */
public final class OccurrenceDwcaMapping {

  private OccurrenceDwcaMapping() {}

  /** Complete Identification History extension for Occurrence core. */
  public static MappingPlan withIdentificationHistory(SchemaGraph graph) {
    MappingPlanBuilder builder =
        mappingPlan("occurrence-core:identification-history", CoreType.OCCURRENCE, "occurrence");
    DirectFieldMappings.from(graph, "occurrence", SchemaPath.root("occurrence")).addTo(builder);
    return builder
        .extension(IdentificationMapping.ROW_TYPE_IDENTIFICATION)
        .importFragment(IdentificationMapping.occurrenceHistory(graph))
        .importFragment(IdentificationMapping.identifiedBy(graph))
        .build();
  }

  /** Direct Occurrence fields plus organism, accepted identification, material/usage-policy, and protocol enrichment. */
  public static MappingPlan withCurrentCoreEnrichment(SchemaGraph graph) {
    return currentCoreBase(graph, "occurrence-core:current-enrichment").build();
  }


  /** Current Occurrence-core enrichments plus direct and material-linked Identifier rows. */
  public static MappingPlan withIdentifiers(SchemaGraph graph) {
    return currentCoreBase(graph, "occurrence-core:identifiers")
        .extension(IdentifierMapping.ROW_TYPE_IDENTIFIER)
        .unionRows()
        .importFragment(IdentifierMapping.occurrenceIdentifiers(graph))
        .importFragment(IdentifierMapping.materialIdentifiersForOccurrence(graph))
        .build();
  }

  /** Canonical currently migrated Occurrence-core mapping used for inspection and replacement wiring. */
  public static MappingPlan current(SchemaGraph graph) {
    MappingPlanBuilder builder = currentCoreBase(graph, "occurrence-core:current");
    builder
        .extension(MultimediaMapping.ROW_TYPE_MULTIMEDIA)
        .unionRows()
        .limitRowsPerParent(50)
        .importFragment(MultimediaMapping.occurrenceMedia(graph))
        .importFragment(MultimediaMapping.materialMediaForOccurrence(graph))
        .endExtension()
        .extension(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT)
        .unionRows()
        .importFragment(AssertionMapping.occurrenceAssertions(graph))
        .importFragment(AssertionMapping.materialAssertionsForOccurrence(graph))
        .importFragment(AssertionMapping.nucleotideAnalysisAssertionsForOccurrence(graph))
        .importFragment(AssertionMapping.molecularProtocolAssertionsForOccurrence(graph))
        .endExtension()
        .extension(IdentificationMapping.ROW_TYPE_IDENTIFICATION)
        .importFragment(IdentificationMapping.occurrenceHistory(graph))
        .importFragment(IdentificationMapping.identifiedBy(graph))
        .endExtension()
        .extension(IdentifierMapping.ROW_TYPE_IDENTIFIER)
        .unionRows()
        .importFragment(IdentifierMapping.occurrenceIdentifiers(graph))
        .importFragment(IdentifierMapping.materialIdentifiersForOccurrence(graph))
        .endExtension()
        .extension(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA)
        .importFragment(NucleotideMapping.materialAnalysesForOccurrence(graph))
        .importFragment(NucleotideMapping.materialAnalysisSequenceForOccurrence(graph))
        .importFragment(NucleotideMapping.materialAnalysisProtocolForOccurrence(graph));
    return builder.build();
  }

  /** Current Occurrence-core enrichments plus DNA analyses linked through the evidence material. */
  public static MappingPlan withNucleotide(SchemaGraph graph) {
    return currentCoreBase(graph, "occurrence-core:nucleotide")
        .extension(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA)
        .importFragment(NucleotideMapping.materialAnalysesForOccurrence(graph))
        .importFragment(NucleotideMapping.materialAnalysisSequenceForOccurrence(graph))
        .importFragment(NucleotideMapping.materialAnalysisProtocolForOccurrence(graph))
        .build();
  }

  /** Current Occurrence-core enrichments plus direct and material-linked eMoF assertion rows. */
  public static MappingPlan withAssertions(SchemaGraph graph) {
    return currentCoreBase(graph, "occurrence-core:assertions")
        .extension(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT)
        .unionRows()
        .importFragment(AssertionMapping.occurrenceAssertions(graph))
        .importFragment(AssertionMapping.materialAssertionsForOccurrence(graph))
        .importFragment(AssertionMapping.nucleotideAnalysisAssertionsForOccurrence(graph))
        .importFragment(AssertionMapping.molecularProtocolAssertionsForOccurrence(graph))
        .build();
  }

  /** Current Occurrence-core enrichments plus direct and material-linked Multimedia rows. */
  public static MappingPlan withMultimedia(SchemaGraph graph) {
    return currentCoreBase(graph, "occurrence-core:multimedia")
        .extension(MultimediaMapping.ROW_TYPE_MULTIMEDIA)
        .unionRows()
        .limitRowsPerParent(50)
        .importFragment(MultimediaMapping.occurrenceMedia(graph))
        .importFragment(MultimediaMapping.materialMediaForOccurrence(graph))
        .build();
  }

  private static MappingPlanBuilder currentCoreBase(SchemaGraph graph, String name) {
    MappingPlanBuilder builder =
        mappingPlan(name, CoreType.OCCURRENCE, "occurrence");
    DirectFieldMappings.from(graph, "occurrence", SchemaPath.root("occurrence")).addTo(builder);
    return builder
        .importCoreFragment(OccurrenceCoreMapping.recordedBy(graph))
        .importCoreFragment(OccurrenceCoreMapping.identifiedBy(graph))
        .importCoreFragment(OccurrenceCoreMapping.organism(graph))
        .importCoreFragment(OccurrenceCoreMapping.acceptedIdentification(graph))
        .importCoreFragment(OccurrenceCoreMapping.acceptedIdentificationAgent(graph))
        .importCoreFragment(OccurrenceCoreMapping.material(graph))
        .importCoreFragment(OccurrenceCoreMapping.materialCollectedBy(graph))
        .importCoreFragment(OccurrenceCoreMapping.materialIdentifiedBy(graph))
        .importCoreFragment(OccurrenceCoreMapping.materialCollectorRoles(graph))
        .importCoreFragment(OccurrenceCoreMapping.materialGeologicalContext(graph))
        .importCoreFragment(OccurrenceCoreMapping.materialProtocols(graph))
        .importCoreFragment(OccurrenceCoreMapping.materialDirectProvenance(graph))
        .importCoreFragment(OccurrenceCoreMapping.materialProvenance(graph))
        .importCoreFragment(OccurrenceCoreMapping.directSamplingProtocol(graph))
        .mergeCoreTarget(org.gbif.dwc.terms.DwcTerm.recordedBy.qualifiedName(), ValueAggregation.firstNonNull())
        .mergeCoreTarget(org.gbif.dwc.terms.DwcTerm.identifiedBy.qualifiedName(), ValueAggregation.firstNonNull())
        .mergeCoreTarget(org.gbif.dwc.terms.DwcTerm.samplingProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
        .mergeCoreTarget(TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("projectID"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("projectTitle"), ValueAggregation.pipeDelimited());
  }
}

