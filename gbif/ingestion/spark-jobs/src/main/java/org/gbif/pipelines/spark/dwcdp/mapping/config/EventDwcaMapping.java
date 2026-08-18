package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder.mappingPlan;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;

/** Event-core mapping configurations assembled from reusable domain fragments. */
public final class EventDwcaMapping {

  private EventDwcaMapping() {}

  /** Canonical currently migrated Event-core mapping used for inspection and replacement wiring. */
  public static MappingPlan current(SchemaGraph graph) {
    MappingPlanBuilder builder =
        eventDirectBase(graph, "event-core:current")
            .mergeCoreTarget(
                DwcTerm.samplingProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
            .mergeCoreTarget(
                DwcTerm.georeferenceProtocol.qualifiedName(),
                ValueAggregation.pipeDelimitedDistinct())
            .mergeCoreTarget(
                TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
            .mergeCoreTarget(
                TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
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
        .mergeTarget(DwcTerm.recordedBy.qualifiedName(), ValueAggregation.firstNonNull())
        .mergeTarget(DwcTerm.identifiedBy.qualifiedName(), ValueAggregation.firstNonNull())
        .mergeTarget(
            DwcTerm.samplingProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
        .mergeTarget(TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("projectID"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("projectTitle"), ValueAggregation.pipeDelimited())
        .importFragment(OccurrenceMapping.directOccurrence(graph))
        .importFragment(OccurrenceMapping.recordedBy(graph))
        .importFragment(OccurrenceMapping.identifiedBy(graph))
        .importFragment(OccurrenceMapping.organism(graph))
        .importFragment(OccurrenceMapping.acceptedIdentification(graph))
        .importFragment(OccurrenceMapping.acceptedIdentificationAgent(graph))
        .importFragment(OccurrenceMapping.material(graph))
        .importFragment(OccurrenceMapping.materialCollectedBy(graph))
        .importFragment(OccurrenceMapping.materialIdentifiedBy(graph))
        .importFragment(OccurrenceMapping.materialCollectorRoles(graph))
        .importFragment(OccurrenceMapping.materialDirectProvenance(graph))
        .importFragment(OccurrenceMapping.materialProvenance(graph))
        .importFragment(OccurrenceMapping.materialGeologicalContext(graph))
        .importFragment(OccurrenceMapping.materialProtocols(graph))
        .endExtension()
        .extension(MultimediaMapping.ROW_TYPE_MULTIMEDIA)
        .unionRows()
        .limitRowsPerParent(50)
        .importFragment(MultimediaMapping.eventMedia(graph))
        .importFragment(MultimediaMapping.occurrenceMediaForEvent(graph))
        .importFragment(MultimediaMapping.materialMediaForEvent(graph))
        .importFragment(MultimediaMapping.chronometricAgeMediaForEvent(graph))
        .endExtension()
        .extension(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT)
        .unionRows()
        .importFragment(AssertionMapping.eventAssertions(graph))
        .importFragment(AssertionMapping.surveyAssertionsForEvent(graph))
        .importFragment(AssertionMapping.nucleotideAnalysisAssertionsForEvent(graph))
        .importFragment(AssertionMapping.molecularProtocolAssertionsForEvent(graph))
        .importFragment(AssertionMapping.chronometricAgeAssertionsForEvent(graph))
        .endExtension()
        .extension(IdentificationMapping.ROW_TYPE_IDENTIFICATION)
        .unionRows()
        .importFragment(IdentificationMapping.eventDnaAnalysisIdentifications(graph))
        .importFragment(IdentificationMapping.eventDnaSequenceIdentifications(graph))
        .endExtension()
        .extension(IdentifierMapping.ROW_TYPE_IDENTIFIER)
        .unionRows()
        .importFragment(IdentifierMapping.eventIdentifiers(graph))
        .importFragment(IdentifierMapping.surveyIdentifiersForEvent(graph))
        .endExtension()
        .extension(ReferenceMapping.ROW_TYPE_REFERENCE)
        .unionRows()
        .importFragment(ReferenceMapping.eventReferences(graph))
        .importFragment(ReferenceMapping.eventSamplingProtocolReferences(graph))
        .importFragment(ReferenceMapping.eventGeoreferenceProtocolReferences(graph))
        .importFragment(ReferenceMapping.eventProtocolReferences(graph))
        .importFragment(ReferenceMapping.surveyReferencesForEvent(graph))
        .importFragment(ReferenceMapping.surveySamplingProtocolReferencesForEvent(graph))
        .importFragment(ReferenceMapping.surveySamplingEffortProtocolReferencesForEvent(graph))
        .importFragment(ReferenceMapping.surveyProtocolReferencesForEvent(graph))
        .importFragment(ReferenceMapping.chronometricAgeReferencesForEvent(graph))
        .importFragment(ReferenceMapping.chronometricAgeProtocolReferencesForEvent(graph))
        .importFragment(ReferenceMapping.chronometricAgeConversionProtocolReferencesForEvent(graph))
        .importFragment(ReferenceMapping.molecularProtocolReferencesForEvent(graph))
        .importFragment(ReferenceMapping.dnaAnalysisIdentificationReferencesForEvent(graph))
        .importFragment(ReferenceMapping.dnaSequenceIdentificationReferencesForEvent(graph))
        .endExtension()
        .extension(HumboldtMapping.ROW_TYPE_HUMBOLDT)
        .importFragment(HumboldtMapping.surveyTargets(graph))
        .importFragment(HumboldtMapping.surveyFields(graph))
        .importFragment(HumboldtMapping.identifiedBy(graph))
        .importFragment(HumboldtMapping.samplingPerformedBy(graph))
        .importFragment(HumboldtMapping.samplingProtocol(graph))
        .importFragment(HumboldtMapping.samplingEffortProtocol(graph))
        .endExtension()
        .extension(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA)
        .importFragment(NucleotideMapping.eventAnalyses(graph))
        .importFragment(NucleotideMapping.eventAnalysisSequence(graph))
        .importFragment(NucleotideMapping.eventAnalysisProtocol(graph))
        .endExtension()
        .extension(ChronometricMapping.ROW_TYPE_CHRONOMETRIC_AGE)
        .importFragment(ChronometricMapping.eventAges(graph))
        .importFragment(ChronometricMapping.ageProtocol(graph))
        .importFragment(ChronometricMapping.conversionProtocol(graph))
        .importFragment(ChronometricMapping.determinedBy(graph));
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
            .importFragment(HumboldtMapping.identifiedBy(graph))
            .importFragment(HumboldtMapping.samplingPerformedBy(graph))
            .importFragment(HumboldtMapping.samplingProtocol(graph))
            .importFragment(HumboldtMapping.samplingEffortProtocol(graph));
    return humboldt.build();
  }

  /**
   * Event-level Multimedia behaviour: direct event media plus occurrence and unambiguous material
   * media promoted to the event.
   */
  public static MappingPlan withMultimedia(SchemaGraph graph) {
    return eventBase("event-core:multimedia")
        .extension(MultimediaMapping.ROW_TYPE_MULTIMEDIA)
        .unionRows()
        .limitRowsPerParent(50)
        .importFragment(MultimediaMapping.eventMedia(graph))
        .importFragment(MultimediaMapping.occurrenceMediaForEvent(graph))
        .importFragment(MultimediaMapping.materialMediaForEvent(graph))
        .importFragment(MultimediaMapping.chronometricAgeMediaForEvent(graph))
        .build();
  }

  /** Current legacy event assertion -> eMoF behaviour. */
  public static MappingPlan withAssertions(SchemaGraph graph) {
    return eventBase("event-core:assertions")
        .extension(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT)
        .unionRows()
        .importFragment(AssertionMapping.eventAssertions(graph))
        .importFragment(AssertionMapping.surveyAssertionsForEvent(graph))
        .importFragment(AssertionMapping.nucleotideAnalysisAssertionsForEvent(graph))
        .importFragment(AssertionMapping.molecularProtocolAssertionsForEvent(graph))
        .importFragment(AssertionMapping.chronometricAgeAssertionsForEvent(graph))
        .build();
  }

  /** Direct Event-owned DNA analyses (eDNA/metabarcoding path). */
  public static MappingPlan withNucleotide(SchemaGraph graph) {
    return eventDirectBase(graph, "event-core:nucleotide")
        .extension(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA)
        .importFragment(NucleotideMapping.eventAnalyses(graph))
        .importFragment(NucleotideMapping.eventAnalysisSequence(graph))
        .importFragment(NucleotideMapping.eventAnalysisProtocol(graph))
        .build();
  }

  /** Event-owned Chronometric Age rows with protocol and determiner-agent enrichment. */
  public static MappingPlan withChronometric(SchemaGraph graph) {
    return eventBase("event-core:chronometric")
        .extension(ChronometricMapping.ROW_TYPE_CHRONOMETRIC_AGE)
        .importFragment(ChronometricMapping.eventAges(graph))
        .importFragment(ChronometricMapping.ageProtocol(graph))
        .importFragment(ChronometricMapping.conversionProtocol(graph))
        .importFragment(ChronometricMapping.determinedBy(graph))
        .build();
  }

  /** Current legacy direct event-identifier behaviour. */
  public static MappingPlan withIdentifier(SchemaGraph graph) {
    return eventBase("event-core:identifier")
        .extension(IdentifierMapping.ROW_TYPE_IDENTIFIER)
        .unionRows()
        .importFragment(IdentifierMapping.eventIdentifiers(graph))
        .importFragment(IdentifierMapping.surveyIdentifiersForEvent(graph))
        .build();
  }

  /** Direct occurrence rows nested below Event core. */
  public static MappingPlan withOccurrenceExtension(SchemaGraph graph) {
    return eventBase("event-core:occurrence")
        .extension(OccurrenceMapping.ROW_TYPE_OCCURRENCE)
        .mergeTarget(DwcTerm.recordedBy.qualifiedName(), ValueAggregation.firstNonNull())
        .mergeTarget(DwcTerm.identifiedBy.qualifiedName(), ValueAggregation.firstNonNull())
        .mergeTarget(
            DwcTerm.samplingProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
        .mergeTarget(TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("projectID"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("projectTitle"), ValueAggregation.pipeDelimited())
        .importFragment(OccurrenceMapping.directOccurrence(graph))
        .importFragment(OccurrenceMapping.recordedBy(graph))
        .importFragment(OccurrenceMapping.identifiedBy(graph))
        .importFragment(OccurrenceMapping.organism(graph))
        .importFragment(OccurrenceMapping.acceptedIdentification(graph))
        .importFragment(OccurrenceMapping.acceptedIdentificationAgent(graph))
        .importFragment(OccurrenceMapping.material(graph))
        .importFragment(OccurrenceMapping.materialCollectedBy(graph))
        .importFragment(OccurrenceMapping.materialIdentifiedBy(graph))
        .importFragment(OccurrenceMapping.materialCollectorRoles(graph))
        .importFragment(OccurrenceMapping.materialDirectProvenance(graph))
        .importFragment(OccurrenceMapping.materialProvenance(graph))
        .importFragment(OccurrenceMapping.materialGeologicalContext(graph))
        .importFragment(OccurrenceMapping.materialProtocols(graph))
        .build();
  }

  /** Direct Event fields plus the non-aggregating Event-core enrichments. */
  public static MappingPlan withCoreEnrichments(SchemaGraph graph) {
    MappingPlanBuilder builder = eventDirectBase(graph, "event-core:core-enrichments");
    return builder
        .mergeCoreTarget(
            DwcTerm.samplingProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
        .mergeCoreTarget(
            DwcTerm.georeferenceProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
        .mergeCoreTarget(
            TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
        .mergeCoreTarget(
            TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
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
    DirectFieldMappings.from(
            graph, "event", SchemaPath.root("event"), TargetTerms.EVENT_CORE_RAW_OUTPUTS)
        .addTo(builder);
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
