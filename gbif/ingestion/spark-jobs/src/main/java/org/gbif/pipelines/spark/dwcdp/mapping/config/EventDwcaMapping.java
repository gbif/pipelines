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
        .mergeTarget(DwcTerm.identifiedByID.qualifiedName(), ValueAggregation.firstNonNull())
        .mergeTarget(TargetTerms.resolve("dateIdentified"), ValueAggregation.firstNonNull())
        .mergeTarget(
            TargetTerms.resolve("identificationReferences"), ValueAggregation.firstNonNull())
        .mergeTarget(
            TargetTerms.resolve("identificationVerificationStatus"),
            ValueAggregation.firstNonNull())
        .mergeTarget(TargetTerms.resolve("identificationRemarks"), ValueAggregation.firstNonNull())
        .mergeTarget(TargetTerms.resolve("verbatimIdentification"), ValueAggregation.firstNonNull())
        .mergeTarget(TargetTerms.resolve("organismScope"), ValueAggregation.firstNonNull())
        .mergeTarget(TargetTerms.resolve("taxonRank"), ValueAggregation.firstNonNull())
        .mergeTarget(TargetTerms.resolve("scientificName"), ValueAggregation.firstNonNull())
        .mergeTarget(TargetTerms.resolve("scientificNameID"), ValueAggregation.firstNonNull())
        .mergeTarget(
            TargetTerms.resolve("scientificNameAuthorship"), ValueAggregation.firstNonNull())
        .mergeTarget(TargetTerms.resolve("vernacularName"), ValueAggregation.firstNonNull())
        .mergeTarget(
            DwcTerm.samplingProtocol.qualifiedName(), ValueAggregation.pipeDelimitedDistinct())
        .mergeTarget(TargetTerms.resolve("fundingAttribution"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("fundingAttributionID"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("projectID"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("projectTitle"), ValueAggregation.pipeDelimited())
        .mergeTarget(TargetTerms.resolve("occurrenceID"), ValueAggregation.firstNonNull())
        .mergeTarget(DwcTerm.eventID.qualifiedName(), ValueAggregation.firstNonNull())
        .importFragment(OccurrenceMapping.directOccurrence(graph))
        .importFragment(OccurrenceMapping.eventIdentity(graph))
        .importFragment(OccurrenceMapping.recordedBy(graph))
        .importFragment(OccurrenceMapping.identifiedBy(graph))
        .importFragment(OccurrenceMapping.organism(graph))
        .importFragment(OccurrenceMapping.acceptedIdentification(graph))
        .importFragment(OccurrenceMapping.acceptedIdentificationAgent(graph))
        .importFragment(OccurrenceMapping.material(graph))
        .importFragment(OccurrenceMapping.acceptedIdentificationTaxon(graph))
        .importFragment(OccurrenceMapping.acceptedIdentificationAgentRoles(graph))
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
        .importFragments(MultimediaMapping.eventFragments(graph))
        .endExtension()
        .extension(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT)
        .unionRows()
        .importFragment(AssertionMapping.eventAssertions(graph))
        .importFragment(AssertionMapping.occurrenceAssertionsForEvent(graph))
        .importFragment(AssertionMapping.materialAssertionsForEvent(graph))
        .importFragment(AssertionMapping.occurrenceNucleotideAnalysisAssertionsForEvent(graph))
        .importFragment(AssertionMapping.occurrenceMolecularProtocolAssertionsForEvent(graph))
        .importFragment(AssertionMapping.surveyAssertionsForEvent(graph))
        .importFragment(AssertionMapping.nucleotideAnalysisAssertionsForEvent(graph))
        .importFragment(AssertionMapping.molecularProtocolAssertionsForEvent(graph))
        .importFragment(AssertionMapping.chronometricAgeAssertionsForEvent(graph))
        .endExtension()
        .extension(IdentificationMapping.ROW_TYPE_IDENTIFICATION)
        .unionRows()
        .importFragment(IdentificationMapping.eventDnaAnalysisIdentifications(graph))
        .importFragment(IdentificationMapping.eventDnaSequenceIdentifications(graph))
        .importFragment(IdentificationMapping.occurrenceHistoryForEvent(graph))
        .importFragment(IdentificationMapping.occurrenceDnaAnalysisIdentificationsForEvent(graph))
        .importFragment(IdentificationMapping.occurrenceDnaSequenceIdentificationsForEvent(graph))
        .endExtension()
        .extension(IdentifierMapping.ROW_TYPE_IDENTIFIER)
        .unionRows()
        .importFragment(IdentifierMapping.eventIdentifiers(graph))
        .importFragment(IdentifierMapping.surveyIdentifiersForEvent(graph))
        .importFragment(IdentifierMapping.occurrenceIdentifiersForEvent(graph))
        .importFragment(IdentifierMapping.materialIdentifiersForEvent(graph))
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
        .importFragment(ReferenceMapping.occurrenceReferencesForEvent(graph))
        .importFragment(ReferenceMapping.occurrenceProtocolReferencesForEvent(graph))
        .importFragment(ReferenceMapping.materialReferencesForEvent(graph))
        .importFragment(ReferenceMapping.materialProtocolReferencesForEvent(graph))
        .importFragment(ReferenceMapping.occurrenceIdentificationReferencesForEvent(graph))
        .importFragment(ReferenceMapping.occurrenceMolecularProtocolReferencesForEvent(graph))
        .importFragment(ReferenceMapping.occurrenceDnaAnalysisIdentificationReferencesForEvent(graph))
        .importFragment(ReferenceMapping.occurrenceDnaSequenceIdentificationReferencesForEvent(graph))
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

  /** Current legacy event assertion -> eMoF behaviour. */
  public static MappingPlan withAssertions(SchemaGraph graph) {
    return eventBase("event-core:assertions")
        .extension(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT)
        .unionRows()
        .importFragment(AssertionMapping.eventAssertions(graph))
        .importFragment(AssertionMapping.occurrenceAssertionsForEvent(graph))
        .importFragment(AssertionMapping.materialAssertionsForEvent(graph))
        .importFragment(AssertionMapping.occurrenceNucleotideAnalysisAssertionsForEvent(graph))
        .importFragment(AssertionMapping.occurrenceMolecularProtocolAssertionsForEvent(graph))
        .importFragment(AssertionMapping.surveyAssertionsForEvent(graph))
        .importFragment(AssertionMapping.nucleotideAnalysisAssertionsForEvent(graph))
        .importFragment(AssertionMapping.molecularProtocolAssertionsForEvent(graph))
        .importFragment(AssertionMapping.chronometricAgeAssertionsForEvent(graph))
        .build();
  }

  /** Compatibility alias retained while the parity migration is in progress. */
  public static MappingPlan withHumboldtSurveyTargets(SchemaGraph graph) {
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
        .coreIdentity(
            ValueAggregation.firstOrUrnFallback("urn:gbif:dwcdp:event:"),
            event.field("eventID"),
            event.field("event_pk"))
        .coreField(
            TargetFieldMapping.oneOf(
                DwcTerm.eventID.qualifiedName(),
                ValueAggregation.firstNonNull(),
                event.field("eventID")));
  }
}
