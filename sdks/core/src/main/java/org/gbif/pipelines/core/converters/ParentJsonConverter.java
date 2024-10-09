package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.factory.SerDeFactory;
import org.gbif.pipelines.core.utils.HashConverter;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFact;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.EventJsonRecord;
import org.gbif.pipelines.io.avro.json.JoinRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.MetadataJsonRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.io.avro.json.Parent;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;

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
  protected OccurrenceJsonRecord occurrenceJsonRecord;
  protected MeasurementOrFactRecord measurementOrFactRecord;

  public ParentJsonRecord convertToParent() {
    return (occurrenceJsonRecord != null) ? convertToParentOccurrence() : convertToParentEvent();
  }

  @SneakyThrows
  public String toJson() {
    return SerDeFactory.avroEventsMapperNonNulls().writeValueAsString(convertToParent());
  }

  /** Converts to parent record based on an event record. */
  private ParentJsonRecord convertToParentEvent() {
    ParentJsonRecord.Builder builder =
        convertToParentRecord()
            .setId(verbatim.getId())
            .setInternalId(identifier.getInternalId())
            .setUniqueKey(identifier.getUniqueKey())
            .setType(ConverterConstants.EVENT)
            .setEventBuilder(convertToEvent())
            .setAll(JsonConverter.convertFieldAll(verbatim, false))
            .setVerbatim(JsonConverter.convertVerbatimEventRecord(verbatim))
            .setJoinRecordBuilder(JoinRecord.newBuilder().setName(ConverterConstants.EVENT));

    mapCreated(builder);
    mapDerivedMetadata(builder);
    mapLocationInheritedFields(builder);
    mapTemporalInheritedFields(builder);
    mapEventInheritedFields(builder);

    JsonConverter.convertToDate(identifier.getFirstLoaded()).ifPresent(builder::setFirstLoaded);

    return builder.build();
  }

  /** Converts to a parent record based on an occurrence record. */
  private ParentJsonRecord convertToParentOccurrence() {
    return convertToParentRecord()
        .setType(ConverterConstants.OCCURRENCE)
        .setId(occurrenceJsonRecord.getId())
        .setInternalId(
            HashConverter.getSha1(
                metadata.getDatasetKey(),
                occurrenceJsonRecord.getVerbatim().getCoreId(),
                occurrenceJsonRecord.getOccurrenceId()))
        .setJoinRecordBuilder(
            JoinRecord.newBuilder()
                .setName(ConverterConstants.OCCURRENCE)
                .setParent(
                    HashConverter.getSha1(
                        metadata.getDatasetKey(), occurrenceJsonRecord.getVerbatim().getCoreId())))
        .setOccurrence(occurrenceJsonRecord)
        .setAll(occurrenceJsonRecord.getAll())
        .setVerbatim(occurrenceJsonRecord.getVerbatim())
        .setCreated(occurrenceJsonRecord.getCreated())
        .build();
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

    mapEventCoreRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapMultimediaRecord(builder);
    mapExtendedRecord(builder);
    mapMeasurementOrFactRecord(builder);

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

    if (eventCore.getParentsLineage() != null && !eventCore.getParentsLineage().isEmpty()) {
      List<String> eventTypes = getParentsLineageEventTypes();
      List<String> eventIDs = getLineageEventIDs();

      builder
          .setEventTypeHierarchy(eventTypes)
          .setEventTypeHierarchyJoined(String.join(ConverterConstants.DELIMITER, eventTypes))
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
      builder.setEventHierarchy(eventHierarchy);

      // add the single type to hierarchy for consistency
      List<String> eventTypeHierarchy = new ArrayList<>();
      if (builder.getEventType() != null && builder.getEventType().getConcept() != null) {
        eventTypeHierarchy.add(builder.getEventType().getConcept());
      }
      builder.setEventTypeHierarchy(eventTypeHierarchy);
    }

    // Vocabulary
    JsonConverter.convertVocabularyConcept(eventCore.getEventType())
        .ifPresent(builder::setEventType);

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

    if (eventCore.getEventType() != null) {
      eventTypes.add(eventCore.getEventType().getConcept());
    } else {
      extractOptValue(verbatim, DwcTerm.eventType).ifPresent(eventTypes::add);
    }

    return eventTypes;
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
            .collect(Collectors.toList()));
    builder.setMeasurementOrFactTypes(
        measurementOrFactRecord.getMeasurementOrFactItems().stream()
            .map(MeasurementOrFact::getMeasurementType)
            .collect(Collectors.toList()));
  }

  private void mapExtendedRecord(EventJsonRecord.Builder builder) {
    builder.setExtensions(JsonConverter.convertExtensions(verbatim));

    // Set raw as indexed
    extractOptValue(verbatim, DwcTerm.eventID).ifPresent(builder::setEventID);
    extractOptValue(verbatim, DwcTerm.institutionCode).ifPresent(builder::setInstitutionCode);
    extractOptValue(verbatim, DwcTerm.verbatimDepth).ifPresent(builder::setVerbatimDepth);
    extractOptValue(verbatim, DwcTerm.verbatimElevation).ifPresent(builder::setVerbatimElevation);

    // Todo: replce with extractOptValue
    String eventName = verbatim.getCoreTerms().get(ConverterConstants.EVENT_NAME);
    Optional.ofNullable(eventName).ifPresent(builder::setEventName);
  }

  private void mapIssues(EventJsonRecord.Builder builder) {
    JsonConverter.mapIssues(
        Arrays.asList(metadata, eventCore, temporal, location, multimedia),
        builder::setIssues,
        builder::setNotIssues);
  }

  private void mapCreated(ParentJsonRecord.Builder builder) {
    JsonConverter.getMaxCreationDate(metadata, eventCore, temporal, location, multimedia)
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
}
