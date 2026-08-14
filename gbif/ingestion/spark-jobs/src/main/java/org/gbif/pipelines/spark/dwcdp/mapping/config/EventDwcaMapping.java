package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.MappingPlanBuilder.mappingPlan;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

/** Event-core mapping configurations assembled from reusable domain fragments. */
public final class EventDwcaMapping {

  private EventDwcaMapping() {}


  /** Canonical currently migrated Event-core mapping used for inspection and replacement wiring. */
  public static MappingPlan current(SchemaGraph graph) {
    MappingPlanBuilder builder = eventDirectBase(graph, "event-core:current")
        .mergeCoreTarget(DwcTerm.samplingProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
        .mergeCoreTarget(DwcTerm.georeferenceProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
        .mergeCoreTarget(TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("projectID"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("projectTitle"), ValueAggregation.pipeDelimited())
        .importCoreFragment(EventCoreMapping.parentEvent(graph))
        .importCoreFragment(EventCoreMapping.geologicalContext(graph))
        .importCoreFragment(EventCoreMapping.eventConductedBy(graph))
        .importCoreFragment(EventCoreMapping.georeferencedBy(graph))
        .importCoreFragment(EventCoreMapping.directSamplingProtocol(graph))
        .importCoreFragment(EventCoreMapping.directGeoreferenceProtocol(graph))
        .importCoreFragment(EventCoreMapping.eventProtocols(graph))
        .importCoreFragment(EventCoreMapping.surveyProtocols(graph))
        .importCoreFragment(EventCoreMapping.eventGeoreferenceProtocols(graph))
        .importCoreFragment(EventCoreMapping.surveyGeoreferenceProtocols(graph))
        .importCoreFragment(EventCoreMapping.directProvenance(graph))
        .importCoreFragment(EventCoreMapping.eventProvenance(graph));

    builder
        .extension(OccurrenceMapping.ROW_TYPE_OCCURRENCE)
        .mergeTarget(TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("projectID"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("projectTitle"), ValueAggregation.pipeDelimited())
        .importFragment(OccurrenceMapping.directOccurrence(graph))
        .importFragment(OccurrenceMapping.organism(graph))
        .importFragment(OccurrenceMapping.acceptedIdentification(graph))
        .importFragment(OccurrenceMapping.material(graph))
        .importFragment(OccurrenceMapping.materialDirectProvenance(graph))
        .importFragment(OccurrenceMapping.materialProvenance(graph))
        .endExtension()
        .extension(MultimediaMapping.ROW_TYPE_MULTIMEDIA)
        .unionRows()
        .limitRowsPerParent(50)
        .importFragment(MultimediaMapping.eventMedia(graph))
        .importFragment(MultimediaMapping.occurrenceMediaForEvent(graph))
        .importFragment(MultimediaMapping.materialMediaForEvent(graph))
        .endExtension()
        .extension(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT)
        .importFragment(AssertionMapping.eventAssertions(graph))
        .endExtension()
        .extension(IdentifierMapping.ROW_TYPE_IDENTIFIER)
        .importFragment(IdentifierMapping.eventIdentifiers(graph))
        .endExtension()
        .extension(HumboldtMapping.ROW_TYPE_HUMBOLDT)
        .importFragment(HumboldtMapping.surveyTargets(graph))
        .importFragment(HumboldtMapping.surveyFields(graph))
        .importFragment(HumboldtMapping.samplingProtocol(graph))
        .importFragment(HumboldtMapping.samplingEffortProtocol(graph));
    return builder.build();
  }

  /**
   * Current legacy Humboldt behaviour. The survey-target path is optional, so one governing plan
   * covers datasets with targets, without targets, and with either optional target table absent.
   */
  public static MappingPlan withHumboldt(SchemaGraph graph) {
    MappingPlanBuilder.ExtensionBuilder humboldt =
        eventBase("event-core:humboldt")
            .extension(HumboldtMapping.ROW_TYPE_HUMBOLDT)
            .importFragment(HumboldtMapping.surveyTargets(graph))
            .importFragment(HumboldtMapping.surveyFields(graph))
            .importFragment(HumboldtMapping.samplingProtocol(graph))
            .importFragment(HumboldtMapping.samplingEffortProtocol(graph));
    return humboldt.build();
  }

  /**
   * Event-level Multimedia behaviour: direct event media plus occurrence and unambiguous
   * material media promoted to the event.
   */
  public static MappingPlan withMultimedia(SchemaGraph graph) {
    return eventBase("event-core:multimedia")
        .extension(MultimediaMapping.ROW_TYPE_MULTIMEDIA)
        .unionRows()
        .limitRowsPerParent(50)
        .importFragment(MultimediaMapping.eventMedia(graph))
        .importFragment(MultimediaMapping.occurrenceMediaForEvent(graph))
        .importFragment(MultimediaMapping.materialMediaForEvent(graph))
        .build();
  }

  /** Current legacy event assertion -> eMoF behaviour. */
  public static MappingPlan withAssertions(SchemaGraph graph) {
    return eventBase("event-core:assertions")
        .extension(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT)
        .importFragment(AssertionMapping.eventAssertions(graph))
        .build();
  }

  /** Current legacy direct event-identifier behaviour. */
  public static MappingPlan withIdentifier(SchemaGraph graph) {
    return eventBase("event-core:identifier")
        .extension(IdentifierMapping.ROW_TYPE_IDENTIFIER)
        .importFragment(IdentifierMapping.eventIdentifiers(graph))
        .build();
  }

  /** Direct occurrence rows nested below Event core. */
  public static MappingPlan withOccurrenceExtension(SchemaGraph graph) {
    return eventBase("event-core:occurrence")
        .extension(OccurrenceMapping.ROW_TYPE_OCCURRENCE)
        .mergeTarget(TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("projectID"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("projectTitle"), ValueAggregation.pipeDelimited())
        .importFragment(OccurrenceMapping.directOccurrence(graph))
        .importFragment(OccurrenceMapping.organism(graph))
        .importFragment(OccurrenceMapping.acceptedIdentification(graph))
        .importFragment(OccurrenceMapping.material(graph))
        .importFragment(OccurrenceMapping.materialDirectProvenance(graph))
        .importFragment(OccurrenceMapping.materialProvenance(graph))
        .build();
  }

  /** Direct Event fields plus the non-aggregating Event-core enrichments. */
  public static MappingPlan withCoreEnrichments(SchemaGraph graph) {
    MappingPlanBuilder builder = eventDirectBase(graph, "event-core:core-enrichments");
    return builder
        .mergeCoreTarget(DwcTerm.samplingProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
        .mergeCoreTarget(DwcTerm.georeferenceProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
        .mergeCoreTarget(TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("projectID"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(TargetTerms.resolve("projectTitle"), ValueAggregation.pipeDelimited())
        .importCoreFragment(EventCoreMapping.parentEvent(graph))
        .importCoreFragment(EventCoreMapping.geologicalContext(graph))
        .importCoreFragment(EventCoreMapping.eventConductedBy(graph))
        .importCoreFragment(EventCoreMapping.georeferencedBy(graph))
        .importCoreFragment(EventCoreMapping.directSamplingProtocol(graph))
        .importCoreFragment(EventCoreMapping.directGeoreferenceProtocol(graph))
        .importCoreFragment(EventCoreMapping.eventProtocols(graph))
        .importCoreFragment(EventCoreMapping.surveyProtocols(graph))
        .importCoreFragment(EventCoreMapping.eventGeoreferenceProtocols(graph))
        .importCoreFragment(EventCoreMapping.surveyGeoreferenceProtocols(graph))
        .importCoreFragment(EventCoreMapping.directProvenance(graph))
        .importCoreFragment(EventCoreMapping.eventProvenance(graph))
        .build();
  }

  /** Compatibility alias retained while the parity migration is in progress. */
  public static MappingPlan withHumboldtSurveyTargets(SchemaGraph graph) {
    return withHumboldt(graph);
  }

  /** Compatibility alias retained while the parity migration is in progress. */
  public static MappingPlan withHumboldtSurveyOnly(SchemaGraph graph) {
    return withHumboldt(graph);
  }

  private static MappingPlanBuilder eventDirectBase(SchemaGraph graph, String name) {
    MappingPlanBuilder builder = eventBase(name);
    DirectFieldMappings.from(graph, "event", SchemaPath.root("event")).addTo(builder);
    return builder;
  }

  private static MappingPlanBuilder eventBase(String name) {
    SchemaPath event = SchemaPath.root("event");
    return mappingPlan(name, CoreType.EVENT, "event")
        .coreField(
            TargetFieldMapping.oneOf(
                DwcTerm.eventID.qualifiedName(),
                ValueAggregation.firstNonNull(),
                event.field("eventID")));
  }
}
