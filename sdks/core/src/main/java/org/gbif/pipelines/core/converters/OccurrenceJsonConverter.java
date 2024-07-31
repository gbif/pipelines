package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractLengthAwareOptValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.factory.SerDeFactory;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.grscicoll.Match;
import org.gbif.pipelines.io.avro.json.GeologicalContext;
import org.gbif.pipelines.io.avro.json.GeologicalRange;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;

@Slf4j
@Builder
public class OccurrenceJsonConverter {

  private final MetadataRecord metadata;
  private final IdentifierRecord identifier;
  private final ClusteringRecord clustering;
  private final BasicRecord basic;
  private final TemporalRecord temporal;
  private final LocationRecord location;
  private final TaxonRecord taxon;
  private final GrscicollRecord grscicoll;
  private final MultimediaRecord multimedia;
  private final ExtendedRecord verbatim;

  public OccurrenceJsonRecord convert() {

    OccurrenceJsonRecord.Builder builder = OccurrenceJsonRecord.newBuilder();

    mapCreated(builder);
    mapIssues(builder);
    mapProjectIds(builder);

    mapMetadataRecord(builder);
    mapIdentifierRecord(builder);
    mapClusteringRecord(builder);
    mapBasicRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapTaxonRecord(builder);
    mapGrscicollRecord(builder);
    mapMultimediaRecord(builder);
    mapExtendedRecord(builder);

    return builder.build();
  }

  @SneakyThrows
  public String toJsonWithNulls() {
    return SerDeFactory.avroMapperWithNulls().writeValueAsString(convert());
  }

  private void mapProjectIds(OccurrenceJsonRecord.Builder builder) {
    Set<String> projectIdsSet = new HashSet<>();

    if (metadata.getProjectId() != null) {
      projectIdsSet.add(metadata.getProjectId());
    }

    if (basic.getProjectId() != null && !basic.getProjectId().isEmpty()) {
      projectIdsSet.addAll(basic.getProjectId());
    }

    if (!projectIdsSet.isEmpty()) {
      List<String> projectIds = new ArrayList<>(projectIdsSet);
      builder.setProjectId(projectIds);
      JsonConverter.convertToMultivalue(projectIds).ifPresent(builder::setProjectIdJoined);
    }
  }

  private void mapMetadataRecord(OccurrenceJsonRecord.Builder builder) {
    builder
        .setCrawlId(metadata.getCrawlId())
        .setDatasetKey(metadata.getDatasetKey())
        .setDatasetTitle(metadata.getDatasetTitle())
        .setDatasetPublishingCountry(metadata.getDatasetPublishingCountry())
        .setEndorsingNodeKey(metadata.getEndorsingNodeKey())
        .setInstallationKey(metadata.getInstallationKey())
        .setHostingOrganizationKey(metadata.getHostingOrganizationKey())
        .setNetworkKeys(metadata.getNetworkKeys())
        .setLicense(metadata.getLicense())
        .setProgrammeAcronym(metadata.getProgrammeAcronym())
        .setProtocol(metadata.getProtocol())
        .setPublisherTitle(metadata.getPublisherTitle())
        .setPublishingOrganizationKey(metadata.getPublishingOrganizationKey());

    JsonConverter.convertToDate(metadata.getLastCrawled()).ifPresent(builder::setLastCrawled);
  }

  private void mapIdentifierRecord(OccurrenceJsonRecord.Builder builder) {
    builder.setGbifId(Long.parseLong(identifier.getInternalId()));
  }

  private void mapClusteringRecord(OccurrenceJsonRecord.Builder builder) {
    builder.setIsClustered(clustering.getIsClustered());
  }

  private void mapBasicRecord(OccurrenceJsonRecord.Builder builder) {

    // Simple
    builder
        .setBasisOfRecord(basic.getBasisOfRecord())
        .setSex(basic.getSex())
        .setIndividualCount(basic.getIndividualCount())
        .setTypeStatus(basic.getTypeStatus())
        .setTypifiedName(basic.getTypifiedName())
        .setSampleSizeValue(basic.getSampleSizeValue())
        .setSampleSizeUnit(basic.getSampleSizeUnit())
        .setOrganismQuantity(basic.getOrganismQuantity())
        .setOrganismQuantityType(basic.getOrganismQuantityType())
        .setRelativeOrganismQuantity(basic.getRelativeOrganismQuantity())
        .setReferences(basic.getReferences())
        .setOccurrenceStatus(basic.getOccurrenceStatus())
        .setIdentifiedBy(JsonConverter.getEscapedList(basic.getIdentifiedBy()))
        .setRecordedBy(JsonConverter.getEscapedList(basic.getRecordedBy()))
        .setDatasetID(JsonConverter.getEscapedList(basic.getDatasetID()))
        .setDatasetName(JsonConverter.getEscapedList(basic.getDatasetName()))
        .setOtherCatalogNumbers(JsonConverter.getEscapedList(basic.getOtherCatalogNumbers()))
        .setPreparations(JsonConverter.getEscapedList(basic.getPreparations()))
        .setSamplingProtocol(JsonConverter.getEscapedList(basic.getSamplingProtocol()))
        .setIsSequenced(basic.getIsSequenced())
        .setAssociatedSequences(basic.getAssociatedSequences())
        // Agent
        .setIdentifiedByIds(JsonConverter.convertAgentList(basic.getIdentifiedByIds()))
        .setRecordedByIds(JsonConverter.convertAgentList(basic.getRecordedByIds()));

    // VocabularyConcept
    JsonConverter.convertVocabularyConcept(basic.getLifeStage()).ifPresent(builder::setLifeStage);
    JsonConverter.convertVocabularyConcept(basic.getEstablishmentMeans())
        .ifPresent(builder::setEstablishmentMeans);
    JsonConverter.convertVocabularyConcept(basic.getDegreeOfEstablishment())
        .ifPresent(builder::setDegreeOfEstablishment);
    JsonConverter.convertVocabularyConcept(basic.getPathway()).ifPresent(builder::setPathway);

    // License
    JsonConverter.convertLicense(basic.getLicense()).ifPresent(builder::setLicense);

    // Multivalue fields
    JsonConverter.convertToMultivalue(basic.getRecordedBy())
        .ifPresent(builder::setRecordedByJoined);
    JsonConverter.convertToMultivalue(basic.getIdentifiedBy())
        .ifPresent(builder::setIdentifiedByJoined);
    JsonConverter.convertToMultivalue(basic.getPreparations())
        .ifPresent(builder::setPreparationsJoined);
    JsonConverter.convertToMultivalue(basic.getSamplingProtocol())
        .ifPresent(builder::setSamplingProtocolJoined);
    JsonConverter.convertToMultivalue(basic.getOtherCatalogNumbers())
        .ifPresent(builder::setOtherCatalogNumbersJoined);

    // Geological context
    org.gbif.pipelines.io.avro.GeologicalContext gx = basic.getGeologicalContext();
    if (gx != null) {

      GeologicalContext.Builder gcb =
          GeologicalContext.newBuilder()
              .setLowestBiostratigraphicZone(gx.getLowestBiostratigraphicZone())
              .setHighestBiostratigraphicZone(gx.getHighestBiostratigraphicZone())
              .setGroup(gx.getGroup())
              .setFormation(gx.getFormation())
              .setMember(gx.getMember())
              .setBed(gx.getBed());

      gcb.setLithostratigraphy(
          Stream.of(gcb.getBed(), gcb.getFormation(), gcb.getGroup(), gcb.getMember())
              .filter(Objects::nonNull)
              .collect(Collectors.toList()));

      gcb.setBiostratigraphy(
          Stream.of(gcb.getLowestBiostratigraphicZone(), gcb.getHighestBiostratigraphicZone())
              .filter(Objects::nonNull)
              .collect(Collectors.toList()));

      JsonConverter.convertVocabularyConcept(gx.getEarliestEonOrLowestEonothem())
          .ifPresent(gcb::setEarliestEonOrLowestEonothem);
      JsonConverter.convertVocabularyConcept(gx.getLatestEonOrHighestEonothem())
          .ifPresent(gcb::setLatestEonOrHighestEonothem);
      JsonConverter.convertVocabularyConcept(gx.getEarliestEraOrLowestErathem())
          .ifPresent(gcb::setEarliestEraOrLowestErathem);
      JsonConverter.convertVocabularyConcept(gx.getLatestEraOrHighestErathem())
          .ifPresent(gcb::setLatestEraOrHighestErathem);
      JsonConverter.convertVocabularyConcept(gx.getEarliestPeriodOrLowestSystem())
          .ifPresent(gcb::setEarliestPeriodOrLowestSystem);
      JsonConverter.convertVocabularyConcept(gx.getLatestPeriodOrHighestSystem())
          .ifPresent(gcb::setLatestPeriodOrHighestSystem);
      JsonConverter.convertVocabularyConcept(gx.getEarliestEpochOrLowestSeries())
          .ifPresent(gcb::setEarliestEpochOrLowestSeries);
      JsonConverter.convertVocabularyConcept(gx.getLatestEpochOrHighestSeries())
          .ifPresent(gcb::setLatestEpochOrHighestSeries);
      JsonConverter.convertVocabularyConcept(gx.getEarliestAgeOrLowestStage())
          .ifPresent(gcb::setEarliestAgeOrLowestStage);
      JsonConverter.convertVocabularyConcept(gx.getLatestAgeOrHighestStage())
          .ifPresent(gcb::setLatestAgeOrHighestStage);

      if (gx.getStartAge() != null && gx.getEndAge() != null) {
        gcb.setRange(
            GeologicalRange.newBuilder().setLte(gx.getStartAge()).setGt(gx.getEndAge()).build());
      }

      builder.setGeologicalContext(gcb.build());
    }
  }

  private void mapTemporalRecord(OccurrenceJsonRecord.Builder builder) {

    builder
        .setYear(temporal.getYear())
        .setMonth(temporal.getMonth())
        .setDay(temporal.getDay())
        .setStartDayOfYear(temporal.getStartDayOfYear())
        .setEndDayOfYear(temporal.getEndDayOfYear())
        .setModified(temporal.getModified())
        .setDateIdentified(temporal.getDateIdentified());

    JsonConverter.convertEventDate(temporal.getEventDate()).ifPresent(builder::setEventDate);
    JsonConverter.convertEventDateSingle(temporal).ifPresent(builder::setEventDateSingle);
    JsonConverter.convertEventDateInterval(temporal).ifPresent(builder::setEventDateInterval);
  }

  private void mapLocationRecord(OccurrenceJsonRecord.Builder builder) {

    builder
        .setContinent(location.getContinent())
        .setWaterBody(location.getWaterBody())
        .setCountry(location.getCountry())
        .setCountryCode(location.getCountryCode())
        .setPublishingCountry(location.getPublishingCountry())
        .setStateProvince(location.getStateProvince())
        .setMinimumElevationInMeters(location.getMinimumElevationInMeters())
        .setMaximumElevationInMeters(location.getMaximumElevationInMeters())
        .setElevation(location.getElevation())
        .setElevationAccuracy(location.getElevationAccuracy())
        .setDepth(location.getDepth())
        .setDepthAccuracy(location.getDepthAccuracy())
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
        .setFootprintWKT(location.getFootprintWKT())
        .setDistanceFromCentroidInMeters(location.getDistanceFromCentroidInMeters())
        .setHigherGeography(location.getHigherGeography())
        .setGeoreferencedBy(location.getGeoreferencedBy())
        .setGbifRegion(location.getGbifRegion())
        .setPublishedByGbifRegion(location.getPublishedByGbifRegion());

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

  private void mapTaxonRecord(OccurrenceJsonRecord.Builder builder) {
    // Set  GbifClassification
    builder.setGbifClassification(JsonConverter.convertClassification(verbatim, taxon));
  }

  private void mapGrscicollRecord(OccurrenceJsonRecord.Builder builder) {

    Optional.ofNullable(grscicoll.getInstitutionMatch())
        .map(Match::getKey)
        .ifPresent(builder::setInstitutionKey);

    Optional.ofNullable(grscicoll.getCollectionMatch())
        .map(Match::getKey)
        .ifPresent(builder::setCollectionKey);
  }

  private void mapMultimediaRecord(OccurrenceJsonRecord.Builder builder) {
    builder
        .setMultimediaItems(JsonConverter.convertMultimediaList(multimedia))
        .setMediaTypes(JsonConverter.convertMultimediaType(multimedia))
        .setMediaLicenses(JsonConverter.convertMultimediaLicense(multimedia));
  }

  private void mapExtendedRecord(OccurrenceJsonRecord.Builder builder) {

    builder
        .setId(verbatim.getId())
        .setAll(JsonConverter.convertFieldAll(verbatim))
        .setExtensions(JsonConverter.convertExtensions(verbatim))
        .setVerbatim(JsonConverter.convertVerbatimRecord(verbatim));

    // Set raw as indexed
    extractLengthAwareOptValue(verbatim, DwcTerm.recordNumber).ifPresent(builder::setRecordNumber);
    extractLengthAwareOptValue(verbatim, DwcTerm.organismID).ifPresent(builder::setOrganismId);
    extractLengthAwareOptValue(verbatim, DwcTerm.eventID).ifPresent(builder::setEventId);
    extractLengthAwareOptValue(verbatim, DwcTerm.parentEventID)
        .ifPresent(builder::setParentEventId);
    extractLengthAwareOptValue(verbatim, DwcTerm.institutionCode)
        .ifPresent(builder::setInstitutionCode);
    extractLengthAwareOptValue(verbatim, DwcTerm.collectionCode)
        .ifPresent(builder::setCollectionCode);
    extractLengthAwareOptValue(verbatim, DwcTerm.catalogNumber)
        .ifPresent(builder::setCatalogNumber);
    extractLengthAwareOptValue(verbatim, DwcTerm.occurrenceID).ifPresent(builder::setOccurrenceId);
    extractLengthAwareOptValue(verbatim, DwcTerm.fieldNumber).ifPresent(builder::setFieldNumber);
    extractLengthAwareOptValue(verbatim, DwcTerm.island).ifPresent(builder::setIsland);
    extractLengthAwareOptValue(verbatim, DwcTerm.islandGroup).ifPresent(builder::setIslandGroup);
    extractLengthAwareOptValue(verbatim, DwcTerm.previousIdentifications)
        .ifPresent(builder::setPreviousIdentifications);
    extractLengthAwareOptValue(verbatim, DwcTerm.taxonConceptID)
        .ifPresent(builder.getGbifClassification()::setTaxonConceptID);
  }

  private void mapIssues(OccurrenceJsonRecord.Builder builder) {
    JsonConverter.mapIssues(
        Arrays.asList(
            metadata,
            identifier,
            clustering,
            basic,
            temporal,
            location,
            taxon,
            grscicoll,
            multimedia),
        builder::setIssues,
        builder::setNotIssues);
  }

  private void mapCreated(OccurrenceJsonRecord.Builder builder) {
    JsonConverter.getMaxCreationDate(
            metadata,
            identifier,
            clustering,
            basic,
            temporal,
            location,
            taxon,
            grscicoll,
            multimedia)
        .ifPresent(builder::setCreated);
  }
}
