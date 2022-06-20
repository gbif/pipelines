package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.utils.HashConverter;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.EventJsonRecord;
import org.gbif.pipelines.io.avro.json.JoinRecord;
import org.gbif.pipelines.io.avro.json.MetadataJsonRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;

@Slf4j
@Builder
public class DenormalizedJsonConverter {

  private final MetadataRecord metadata;
  private final IdentifierRecord identifier;
  private final EventCoreRecord eventCore;
  private final TemporalRecord temporal;
  private final LocationRecord location;
  private final TaxonRecord taxon;
  private final GrscicollRecord grscicoll;
  private final MultimediaRecord multimedia;
  private final ExtendedRecord verbatim;
  private OccurrenceJsonRecord occurrenceJsonRecord;

  public List<ParentJsonRecord> convertToParents() {

    List<ParentJsonRecord> result = new ArrayList<>();
    if (occurrenceJsonRecord != null) {
      result.add(convertToParentOccurrence());
    } else {
      result.add(convertToParentEvent());
    }

    return result;
  }

  public List<String> toJsons() {
    return convertToParents().stream().map(ParentJsonRecord::toString).collect(Collectors.toList());
  }

  private ParentJsonRecord convertToParentEvent() {
    return convertToParent()
        .setType("event")
        .setJoinRecordBuilder(JoinRecord.newBuilder().setName("event"))
        .setEventBuilder(convertToEvent())
        .build();
  }

  private ParentJsonRecord convertToParentOccurrence() {
    return convertToParent()
        .setType("occurrence")
        .setInternalId(
            HashConverter.getSha1(
                metadata.getDatasetKey(), verbatim.getId(), occurrenceJsonRecord.getOccurrenceId()))
        .setJoinRecordBuilder(
            JoinRecord.newBuilder().setName("occurrence").setParent(identifier.getInternalId()))
        .setEventBuilder(convertToEvent())
        .setOccurrence(occurrenceJsonRecord)
        .build();
  }

  private ParentJsonRecord.Builder convertToParent() {
    ParentJsonRecord.Builder builder =
        ParentJsonRecord.newBuilder()
            .setId(verbatim.getId())
            .setCrawlId(metadata.getCrawlId())
            .setInternalId(identifier.getInternalId())
            .setUniqueKey(identifier.getUniqueKey())
            .setMetadataBuilder(mapMetadataJsonRecord());

    mapCreated(builder);

    JsonConverter.convertToDate(identifier.getFirstLoaded()).ifPresent(builder::setFirstLoaded);
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

    // Simple
    builder
        .setSampleSizeValue(eventCore.getSampleSizeValue())
        .setSampleSizeUnit(eventCore.getSampleSizeUnit())
        .setReferences(eventCore.getReferences())
        .setDatasetID(eventCore.getDatasetID())
        .setDatasetName(eventCore.getDatasetName())
        .setSamplingProtocol(eventCore.getSamplingProtocol());

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

  private void mapMultimediaRecord(EventJsonRecord.Builder builder) {
    builder
        .setMultimediaItems(JsonConverter.convertMultimediaList(multimedia))
        .setMediaTypes(JsonConverter.convertMultimediaType(multimedia))
        .setMediaLicenses(JsonConverter.convertMultimediaLicense(multimedia));
  }

  private void mapExtendedRecord(EventJsonRecord.Builder builder) {
    builder.setExtensions(JsonConverter.convertExtensions(verbatim));

    // Set raw as indexed
    extractOptValue(verbatim, DwcTerm.eventID).ifPresent(builder::setEventID);
    extractOptValue(verbatim, DwcTerm.parentEventID).ifPresent(builder::setParentEventID);
    extractOptValue(verbatim, DwcTerm.institutionCode).ifPresent(builder::setInstitutionCode);
    extractOptValue(verbatim, DwcTerm.verbatimDepth).ifPresent(builder::setVerbatimDepth);
    extractOptValue(verbatim, DwcTerm.verbatimElevation).ifPresent(builder::setVerbatimElevation);
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
}
