package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.utils.EventsUtils.*;
import static org.gbif.pipelines.core.utils.ModelUtils.extractLengthAwareOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.DurationUnit;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.factory.SerDeFactory;
import org.gbif.pipelines.core.utils.SortUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.EventJsonRecord;
import org.gbif.pipelines.io.avro.json.Humboldt;
import org.gbif.pipelines.io.avro.json.HumboldtTaxonClassification;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.MetadataJsonRecord;
import org.gbif.pipelines.io.avro.json.Parent;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;
import org.gbif.pipelines.io.avro.json.VocabularyConcept;

@Slf4j
@Builder
public class ParentJsonConverter {

  protected final MetadataRecord metadata;
  protected final IdentifierRecord identifier;
  protected final EventCoreRecord eventCore;
  protected final TemporalRecord temporal;
  protected final LocationRecord location;
  protected final GrscicollRecord grscicoll;
  protected final MultimediaRecord multimedia;
  protected final ExtendedRecord verbatim;
  protected final DerivedMetadataRecord derivedMetadata;
  protected final LocationInheritedRecord locationInheritedRecord;
  protected final TemporalInheritedRecord temporalInheritedRecord;
  protected final EventInheritedRecord eventInheritedRecord;
  protected MeasurementOrFactRecord measurementOrFactRecord;
  protected final HumboldtRecord humboldtRecord;

  @SneakyThrows
  public String toJson() {
    return SerDeFactory.avroEventsMapperNonNulls().writeValueAsString(convertToParent());
  }

  /** Converts to parent record based on an event record. */
  public ParentJsonRecord convertToParent() {
    ParentJsonRecord.Builder builder =
        convertToParentRecord()
            .setId(verbatim.getId())
            .setInternalId(identifier.getInternalId())
            .setUniqueKey(identifier.getUniqueKey())
            .setType(ConverterConstants.EVENT)
            .setEventBuilder(convertToEvent())
            .setAll(JsonConverter.convertFieldAll(verbatim, false))
            .setVerbatim(JsonConverter.convertVerbatimEventRecord(verbatim));

    mapCreated(builder);
    mapDerivedMetadata(builder);
    mapLocationInheritedFields(builder);
    mapTemporalInheritedFields(builder);
    mapEventInheritedFields(builder);

    JsonConverter.convertToDate(identifier.getFirstLoaded()).ifPresent(builder::setFirstLoaded);

    return builder.build();
  }

  /** Converts to a parent record */
  private ParentJsonRecord.Builder convertToParentRecord() {
    ParentJsonRecord.Builder builder =
        ParentJsonRecord.newBuilder()
            .setCrawlId(metadata.getCrawlId())
            .setMetadataBuilder(mapMetadataJsonRecord());

    JsonConverter.convertToDate(metadata.getLastCrawled()).ifPresent(builder::setLastCrawled);

    return builder;
  }

  private EventJsonRecord.Builder convertToEvent() {

    EventJsonRecord.Builder builder = EventJsonRecord.newBuilder();

    builder.setId(verbatim.getId());
    mapIssues(builder);

    mapExtendedRecord(builder);
    mapEventCoreRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapMultimediaRecord(builder);
    mapMeasurementOrFactRecord(builder);
    mapHumboldtRecord(builder);
    mapSortField(builder);

    return builder;
  }

  private MetadataJsonRecord.Builder mapMetadataJsonRecord() {
    return MetadataJsonRecord.newBuilder()
        .setDatasetKey(metadata.getDatasetKey())
        .setDatasetTitle(metadata.getDatasetTitle())
        .setDatasetPublishingCountry(metadata.getDatasetPublishingCountry())
        .setEndorsingNodeKey(metadata.getEndorsingNodeKey())
        .setInstallationKey(metadata.getInstallationKey())
        .setHostingOrganizationKey(metadata.getHostingOrganizationKey())
        .setNetworkKeys(metadata.getNetworkKeys())
        .setProgrammeAcronym(metadata.getProgrammeAcronym())
        .setProjectId(metadata.getProjectId())
        .setProtocol(metadata.getProtocol())
        .setPublisherTitle(metadata.getPublisherTitle())
        .setPublishingOrganizationKey(metadata.getPublishingOrganizationKey());
  }

  private void mapEventCoreRecord(EventJsonRecord.Builder builder) {

    if (eventCore.getEventType() != null
        && eventCore.getEventType().getConcept().equalsIgnoreCase(ConverterConstants.SURVEY)) {
      builder.setSurveyID(builder.getEventID());
    }

    // Simple
    builder
        .setSampleSizeValue(eventCore.getSampleSizeValue())
        .setSampleSizeUnit(eventCore.getSampleSizeUnit())
        .setReferences(eventCore.getReferences())
        .setDatasetID(eventCore.getDatasetID())
        .setDatasetName(eventCore.getDatasetName())
        .setSamplingProtocol(eventCore.getSamplingProtocol())
        .setParentsLineage(convertParents(eventCore.getParentsLineage()))
        .setParentEventID(eventCore.getParentEventID())
        .setLocationID(eventCore.getLocationID());

    // Vocabulary
    JsonConverter.convertVocabularyConcept(eventCore.getEventType())
        .ifPresent(builder::setEventType);

    builder.setVerbatimEventType(
        extractOptValue(verbatim, DwcTerm.eventType).orElse(DEFAULT_EVENT_TYPE));

    if (eventCore.getParentsLineage() != null && !eventCore.getParentsLineage().isEmpty()) {
      List<String> eventTypes = getParentsLineageEventTypes();
      List<String> verbatimEventTypes = getParentsLineageVerbatimEventTypes();
      List<String> eventIDs = getLineageEventIDs();

      builder
          .setEventTypeHierarchy(eventTypes)
          .setEventTypeHierarchyJoined(String.join(ConverterConstants.DELIMITER, eventTypes))
          .setVerbatimEventTypeHierarchy(verbatimEventTypes)
          .setVerbatimEventTypeHierarchyJoined(
              String.join(ConverterConstants.DELIMITER, verbatimEventTypes))
          .setEventHierarchy(eventIDs)
          .setEventHierarchyJoined(String.join(ConverterConstants.DELIMITER, eventIDs))
          .setEventHierarchyLevels(eventIDs.size());

      if (builder.getSurveyID() == null) {
        List<org.gbif.pipelines.io.avro.Parent> surveys =
            eventCore.getParentsLineage().stream()
                .filter(
                    e ->
                        e.getEventType() != null
                            && e.getEventType().equalsIgnoreCase(ConverterConstants.SURVEY))
                .collect(Collectors.toList());
        if (!surveys.isEmpty()) {
          builder.setSurveyID(surveys.get(0).getId());
        }
      }
    } else {
      // add the eventID and parentEventID to hierarchy for consistency
      List<String> eventHierarchy = new ArrayList<>();
      Optional.ofNullable(builder.getParentEventID()).ifPresent(eventHierarchy::add);
      Optional.ofNullable(builder.getEventID()).ifPresent(eventHierarchy::add);

      builder
          .setEventHierarchy(eventHierarchy)
          .setEventHierarchyJoined(String.join(ConverterConstants.DELIMITER, eventHierarchy))
          .setEventHierarchyLevels(eventHierarchy.size());

      // add the single type to hierarchy for consistency
      List<String> eventTypeHierarchy = new ArrayList<>();
      if (builder.getEventType() != null && builder.getEventType().getConcept() != null) {
        eventTypeHierarchy.add(builder.getEventType().getConcept());
      }

      builder
          .setEventTypeHierarchy(eventTypeHierarchy)
          .setEventTypeHierarchyJoined(
              String.join(ConverterConstants.DELIMITER, eventTypeHierarchy));

      List<String> verbatimEventTypeHierarchy = new ArrayList<>();
      if (builder.getVerbatimEventType() != null) {
        verbatimEventTypeHierarchy.add(builder.getVerbatimEventType());
      }

      builder
          .setVerbatimEventTypeHierarchy(verbatimEventTypeHierarchy)
          .setVerbatimEventTypeHierarchyJoined(
              String.join(ConverterConstants.DELIMITER, verbatimEventTypeHierarchy));
    }

    // License
    JsonConverter.convertLicense(eventCore.getLicense()).ifPresent(builder::setLicense);

    // Multivalue fields
    JsonConverter.convertToMultivalue(eventCore.getSamplingProtocol())
        .ifPresent(builder::setSamplingProtocolJoined);
  }

  private void mapTemporalRecord(EventJsonRecord.Builder builder) {

    builder
        .setYear(temporal.getYear())
        .setMonth(temporal.getMonth())
        .setDay(temporal.getDay())
        .setStartDayOfYear(temporal.getStartDayOfYear())
        .setEndDayOfYear(temporal.getEndDayOfYear())
        .setModified(temporal.getModified());

    JsonConverter.convertEventDate(temporal.getEventDate()).ifPresent(builder::setEventDate);
    JsonConverter.convertEventDateSingle(temporal).ifPresent(builder::setEventDateSingle);
    JsonConverter.convertEventDateInterval(temporal).ifPresent(builder::setEventDateInterval);
  }

  private void mapLocationRecord(EventJsonRecord.Builder builder) {

    builder
        .setContinent(location.getContinent())
        .setWaterBody(location.getWaterBody())
        .setCountry(location.getCountry())
        .setCountryCode(location.getCountryCode())
        .setPublishingCountry(location.getPublishingCountry())
        .setStateProvince(location.getStateProvince())
        .setMinimumElevationInMeters(location.getMinimumElevationInMeters())
        .setMaximumElevationInMeters(location.getMaximumElevationInMeters())
        .setMinimumDepthInMeters(location.getMinimumDepthInMeters())
        .setMaximumDepthInMeters(location.getMaximumDepthInMeters())
        .setMaximumDistanceAboveSurfaceInMeters(location.getMaximumDistanceAboveSurfaceInMeters())
        .setMinimumDistanceAboveSurfaceInMeters(location.getMinimumDistanceAboveSurfaceInMeters())
        .setCoordinateUncertaintyInMeters(location.getCoordinateUncertaintyInMeters())
        .setCoordinatePrecision(location.getCoordinatePrecision())
        .setHasCoordinate(location.getHasCoordinate())
        .setRepatriated(location.getRepatriated())
        .setHasGeospatialIssue(location.getHasGeospatialIssue())
        .setLocality(location.getLocality())
        .setFootprintWKT(location.getFootprintWKT());

    // Coordinates
    Double decimalLongitude = location.getDecimalLongitude();
    Double decimalLatitude = location.getDecimalLatitude();
    if (decimalLongitude != null && decimalLatitude != null) {
      builder
          .setDecimalLatitude(decimalLatitude)
          .setDecimalLongitude(decimalLongitude)
          // geo_point
          .setCoordinates(JsonConverter.convertCoordinates(decimalLongitude, decimalLatitude))
          // geo_shape
          .setScoordinates(JsonConverter.convertScoordinates(decimalLongitude, decimalLatitude));
    }

    JsonConverter.convertGadm(location.getGadm()).ifPresent(builder::setGadm);
  }

  private List<String> getParentsLineageEventTypes() {
    List<String> eventTypes =
        eventCore.getParentsLineage().stream()
            .sorted(Comparator.comparingInt(org.gbif.pipelines.io.avro.Parent::getOrder).reversed())
            .map(org.gbif.pipelines.io.avro.Parent::getEventType)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // eventType can't be null, the interpretation uses a fallback
    eventTypes.add(eventCore.getEventType().getConcept());

    return eventTypes;
  }

  private List<String> getParentsLineageVerbatimEventTypes() {
    List<String> verbatimEventTypes =
        eventCore.getParentsLineage().stream()
            .sorted(Comparator.comparingInt(org.gbif.pipelines.io.avro.Parent::getOrder).reversed())
            .map(org.gbif.pipelines.io.avro.Parent::getVerbatimEventType)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    verbatimEventTypes.add(extractOptValue(verbatim, DwcTerm.eventType).orElse(DEFAULT_EVENT_TYPE));

    return verbatimEventTypes;
  }

  private List<String> getLineageEventIDs() {

    List<String> eventIDs =
        eventCore.getParentsLineage().stream()
            .sorted(Comparator.comparingInt(org.gbif.pipelines.io.avro.Parent::getOrder).reversed())
            .map(org.gbif.pipelines.io.avro.Parent::getId)
            .collect(Collectors.toList());
    eventIDs.add(eventCore.getId());
    return eventIDs;
  }

  private void mapMultimediaRecord(EventJsonRecord.Builder builder) {
    builder
        .setMultimediaItems(JsonConverter.convertMultimediaList(multimedia))
        .setMediaTypes(JsonConverter.convertMultimediaType(multimedia))
        .setMediaLicenses(JsonConverter.convertMultimediaLicense(multimedia));
  }

  private void mapMeasurementOrFactRecord(EventJsonRecord.Builder builder) {
    builder.setMeasurementOrFactMethods(
        measurementOrFactRecord.getMeasurementOrFactItems().stream()
            .map(MeasurementOrFact::getMeasurementMethod)
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.toList()));
    builder.setMeasurementOrFactTypes(
        measurementOrFactRecord.getMeasurementOrFactItems().stream()
            .map(MeasurementOrFact::getMeasurementType)
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.toList()));
  }

  private void mapHumboldtRecord(EventJsonRecord.Builder builder) {
    builder.setHumboldt(
        humboldtRecord.getHumboldtItems().stream()
            .map(
                h -> {
                  Humboldt.Builder humboldtBuilder =
                      Humboldt.newBuilder()
                          .setSiteCount(h.getSiteCount())
                          .setVerbatimSiteDescriptions(h.getVerbatimSiteDescriptions())
                          .setVerbatimSiteNames(h.getVerbatimSiteNames())
                          .setGeospatialScopeAreaValue(h.getGeospatialScopeAreaValue())
                          .setGeospatialScopeAreaUnit(h.getGeospatialScopeAreaUnit())
                          .setTotalAreaSampledValue(h.getTotalAreaSampledValue())
                          .setTotalAreaSampledUnit(h.getTotalAreaSampledUnit())
                          .setEventDurationValue(h.getEventDurationValue())
                          .setEventDurationUnit(h.getEventDurationUnit())
                          .setGeospatialScopeAreaUnit(h.getGeospatialScopeAreaUnit())
                          .setTaxonCompletenessProtocols(h.getTaxonCompletenessProtocols())
                          .setIsTaxonomicScopeFullyReported(h.getIsTaxonomicScopeFullyReported())
                          .setIsAbsenceReported(h.getIsAbsenceReported())
                          .setHasNonTargetTaxa(h.getHasNonTargetTaxa())
                          .setAreNonTargetTaxaFullyReported(h.getAreNonTargetTaxaFullyReported())
                          .setIsLifeStageScopeFullyReported(h.getIsLifeStageScopeFullyReported())
                          .setIsDegreeOfEstablishmentScopeFullyReported(
                              h.getIsDegreeOfEstablishmentScopeFullyReported())
                          .setIsGrowthFormScopeFullyReported(h.getIsGrowthFormScopeFullyReported())
                          .setHasNonTargetOrganisms(h.getHasNonTargetOrganisms())
                          .setCompilationTypes(h.getCompilationTypes())
                          .setCompilationSourceTypes(h.getCompilationSourceTypes())
                          .setInventoryTypes(h.getInventoryTypes())
                          .setProtocolNames(h.getProtocolNames())
                          .setProtocolDescriptions(h.getProtocolDescriptions())
                          .setProtocolReferences(h.getProtocolReferences())
                          .setIsAbundanceReported(h.getIsAbundanceReported())
                          .setIsAbundanceCapReported(h.getIsAbundanceCapReported())
                          .setAbundanceCap(h.getAbundanceCap())
                          .setIsVegetationCoverReported(h.getIsVegetationCoverReported())
                          .setIsLeastSpecificTargetCategoryQuantityInclusive(
                              h.getIsLeastSpecificTargetCategoryQuantityInclusive())
                          .setHasVouchers(h.getHasVouchers())
                          .setVoucherInstitutions(h.getVoucherInstitutions())
                          .setHasMaterialSamples(h.getHasMaterialSamples())
                          .setMaterialSampleTypes(h.getMaterialSampleTypes())
                          .setSamplingPerformedBy(h.getSamplingPerformedBy())
                          .setIsSamplingEffortReported(h.getIsSamplingEffortReported())
                          .setSamplingEffortValue(h.getSamplingEffortValue())
                          .setSamplingEffortUnit(h.getSamplingEffortUnit());

                  Function<
                          List<org.gbif.pipelines.io.avro.VocabularyConcept>,
                          List<VocabularyConcept>>
                      vocabConverter =
                          vocabs ->
                              vocabs.stream()
                                  .map(
                                      v ->
                                          VocabularyConcept.newBuilder()
                                              .setConcept(v.getConcept())
                                              .setLineage(v.getLineage())
                                              .build())
                                  .collect(Collectors.toList());

                  humboldtBuilder.setTargetHabitatScope(h.getTargetHabitatScope());
                  humboldtBuilder.setExcludedHabitatScope(h.getExcludedHabitatScope());
                  humboldtBuilder.setTargetGrowthFormScope(h.getTargetGrowthFormScope());
                  humboldtBuilder.setExcludedGrowthFormScope(h.getExcludedGrowthFormScope());
                  humboldtBuilder.setTargetLifeStageScope(
                      vocabConverter.apply(h.getTargetLifeStageScope()));
                  humboldtBuilder.setExcludedLifeStageScope(
                      vocabConverter.apply(h.getExcludedLifeStageScope()));
                  humboldtBuilder.setTargetDegreeOfEstablishmentScope(
                      vocabConverter.apply(h.getTargetDegreeOfEstablishmentScope()));
                  humboldtBuilder.setExcludedDegreeOfEstablishmentScope(
                      vocabConverter.apply(h.getExcludedDegreeOfEstablishmentScope()));

                  if (h.getEventDurationValue() != null && h.getEventDurationUnit() != null) {
                    DurationUnit eventDurationUnit = DurationUnit.valueOf(h.getEventDurationUnit());
                    humboldtBuilder.setEventDurationValueInMinutes(
                        h.getEventDurationValue() * eventDurationUnit.getDurationInMinutes());
                  }

                  // taxon
                  Function<
                          List<TaxonHumboldtRecord>, Map<String, List<HumboldtTaxonClassification>>>
                      taxonFn =
                          taxonRecords -> {
                            Map<String, List<HumboldtTaxonClassification>> classifications =
                                new HashMap<>();
                            taxonRecords.forEach(
                                t -> {
                                  HumboldtTaxonClassification.Builder taxonBuilder =
                                      HumboldtTaxonClassification.newBuilder()
                                          .setUsageKey(t.getUsageKey())
                                          .setUsageName(t.getUsageName())
                                          .setUsageRank(t.getUsageRank());

                                  Map<String, String> classification = new HashMap<>();
                                  Map<String, String> classificationKeys = new HashMap<>();
                                  List<String> taxonKeys = new ArrayList<>();
                                  t.getClassification()
                                      .forEach(
                                          c -> {
                                            classification.put(c.getRank(), c.getName());
                                            classificationKeys.put(c.getRank(), c.getKey());
                                            taxonKeys.add(c.getKey());
                                          });

                                  taxonBuilder.setClassification(classification);
                                  taxonBuilder.setClassificationKeys(classificationKeys);
                                  taxonBuilder.setTaxonKeys(taxonKeys);
                                  taxonBuilder.setIssues(t.getIssues().getIssueList());
                                  classifications
                                      .computeIfAbsent(t.getChecklistKey(), k -> new ArrayList<>())
                                      .add(taxonBuilder.build());
                                });
                            return classifications;
                          };

                  humboldtBuilder.setTargetTaxonomicScope(
                      taxonFn.apply(h.getTargetTaxonomicScope()));
                  humboldtBuilder.setExcludedTaxonomicScope(
                      taxonFn.apply(h.getExcludedTaxonomicScope()));
                  humboldtBuilder.setAbsentTaxa(taxonFn.apply(h.getAbsentTaxa()));
                  humboldtBuilder.setNonTargetTaxa(taxonFn.apply(h.getNonTargetTaxa()));

                  return humboldtBuilder.build();
                })
            .collect(Collectors.toList()));
  }

  private void mapExtendedRecord(EventJsonRecord.Builder builder) {
    builder.setExtensions(JsonConverter.convertExtensions(verbatim));

    // Set raw as indexed
    extractOptValue(verbatim, DwcTerm.eventID).ifPresent(builder::setEventID);
    extractOptValue(verbatim, DwcTerm.institutionCode).ifPresent(builder::setInstitutionCode);
    extractOptValue(verbatim, DwcTerm.verbatimDepth).ifPresent(builder::setVerbatimDepth);
    extractOptValue(verbatim, DwcTerm.verbatimElevation).ifPresent(builder::setVerbatimElevation);
    extractLengthAwareOptValue(verbatim, DwcTerm.fieldNumber).ifPresent(builder::setFieldNumber);

    // Todo: replce with extractOptValue
    String eventName = verbatim.getCoreTerms().get(ConverterConstants.EVENT_NAME);
    Optional.ofNullable(eventName).ifPresent(builder::setEventName);
  }

  private void mapIssues(EventJsonRecord.Builder builder) {
    JsonConverter.mapIssues(
        Arrays.asList(metadata, eventCore, temporal, location, multimedia, humboldtRecord),
        builder::setIssues,
        builder::setNotIssues);
  }

  private void mapCreated(ParentJsonRecord.Builder builder) {
    JsonConverter.getMaxCreationDate(
            metadata, eventCore, temporal, location, multimedia, humboldtRecord)
        .ifPresent(builder::setCreated);
  }

  private void mapDerivedMetadata(ParentJsonRecord.Builder builder) {
    builder.setDerivedMetadata(derivedMetadata);
  }

  private void mapLocationInheritedFields(ParentJsonRecord.Builder builder) {
    if (locationInheritedRecord.getId() != null) {
      builder.setLocationInherited(locationInheritedRecord);
    }
  }

  private void mapTemporalInheritedFields(ParentJsonRecord.Builder builder) {
    if (temporalInheritedRecord.getId() != null) {
      builder.setTemporalInherited(temporalInheritedRecord);
    }
  }

  private void mapEventInheritedFields(ParentJsonRecord.Builder builder) {
    if (eventInheritedRecord.getId() != null) {
      builder.setEventInherited(eventInheritedRecord);
    }
  }

  protected static List<Parent> convertParents(List<org.gbif.pipelines.io.avro.Parent> parents) {
    if (parents == null) {
      return Collections.emptyList();
    }

    return parents.stream()
        .map(p -> Parent.newBuilder().setId(p.getId()).setEventType(p.getEventType()).build())
        .collect(Collectors.toList());
  }

  private void mapSortField(EventJsonRecord.Builder builder) {
    builder.setYearMonthEventIDSort(
        SortUtils.yearDescMonthAscEventIDAscSortKey(
            builder.getYear(), builder.getMonth(), builder.getEventID()));
  }
}
