package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.MappingPlanBuilder.mappingPlan;

import org.gbif.pipelines.spark.dwcdp.mapping.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

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
        .build();
  }

  /** Direct Occurrence fields plus organism, accepted identification, material/usage-policy, and protocol enrichment. */
  public static MappingPlan withCurrentCoreEnrichment(SchemaGraph graph) {
    return currentCoreBase(graph, "occurrence-core:current-enrichment").build();
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
        .importCoreFragment(OccurrenceCoreMapping.organism(graph))
        .importCoreFragment(OccurrenceCoreMapping.acceptedIdentification(graph))
        .importCoreFragment(OccurrenceCoreMapping.material(graph))
        .importCoreFragment(OccurrenceCoreMapping.materialDirectProvenance(graph))
        .importCoreFragment(OccurrenceCoreMapping.materialProvenance(graph))
        .importCoreFragment(OccurrenceCoreMapping.directSamplingProtocol(graph))
        .mergeCoreTarget(TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("projectID"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("projectTitle"), ValueAggregation.pipeDelimited());
  }
}

