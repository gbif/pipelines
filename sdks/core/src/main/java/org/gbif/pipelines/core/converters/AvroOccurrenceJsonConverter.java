package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.util.Arrays;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.grscicoll.Match;
import org.gbif.pipelines.io.avro.json.GbifClassification;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;

@Slf4j
@Builder
public class AvroOccurrenceJsonConverter {

  private final MetadataRecord metadataRecord;
  private final BasicRecord basicRecord;
  private final TemporalRecord temporalRecord;
  private final LocationRecord locationRecord;
  private final TaxonRecord taxonRecord;
  private final GrscicollRecord grscicollRecord;
  private final MultimediaRecord multimediaRecord;
  private final ExtendedRecord extendedRecord;

  public OccurrenceJsonRecord convert() {

    OccurrenceJsonRecord.Builder builder = OccurrenceJsonRecord.newBuilder();

    mapCreated(builder);
    mapIssues(builder);

    mapMetadataRecord(builder);
    mapBasicRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapTaxonRecord(builder);
    mapGrscicollRecord(builder);
    mapMultimediaRecord(builder);
    mapExtendedRecord(builder);

    return builder.build();
  }

  private void mapMetadataRecord(OccurrenceJsonRecord.Builder builder) {
    builder.setCrawlId(metadataRecord.getCrawlId());
    builder.setDatasetKey(metadataRecord.getDatasetKey());
    builder.setDatasetTitle(metadataRecord.getDatasetTitle());
    builder.setDatasetPublishingCountry(metadataRecord.getDatasetPublishingCountry());
    builder.setEndorsingNodeKey(metadataRecord.getEndorsingNodeKey());
    builder.setInstallationKey(metadataRecord.getInstallationKey());
    builder.setHostingOrganizationKey(metadataRecord.getHostingOrganizationKey());
    builder.setNetworkKeys(metadataRecord.getNetworkKeys());
    builder.setProgrammeAcronym(metadataRecord.getProgrammeAcronym());
    builder.setProjectId(metadataRecord.getProjectId());
    builder.setProtocol(metadataRecord.getProtocol());
    builder.setPublisherTitle(metadataRecord.getPublisherTitle());
    builder.setPublishingOrganizationKey(metadataRecord.getPublishingOrganizationKey());

    JsonConverter.convertToDate(metadataRecord.getLastCrawled()).ifPresent(builder::setLastCrawled);
  }

  private void mapBasicRecord(OccurrenceJsonRecord.Builder builder) {

    // Simple
    builder.setGbifId(basicRecord.getGbifId());
    builder.setBasisOfRecord(basicRecord.getBasisOfRecord());
    builder.setSex(basicRecord.getSex());
    builder.setIndividualCount(basicRecord.getIndividualCount());
    builder.setTypeStatus(basicRecord.getTypeStatus());
    builder.setTypifiedName(basicRecord.getTypifiedName());
    builder.setSampleSizeValue(basicRecord.getSampleSizeValue());
    builder.setSampleSizeUnit(basicRecord.getSampleSizeUnit());
    builder.setOrganismQuantity(basicRecord.getOrganismQuantity());
    builder.setOrganismQuantityType(basicRecord.getOrganismQuantityType());
    builder.setRelativeOrganismQuantity(basicRecord.getRelativeOrganismQuantity());
    builder.setReferences(basicRecord.getReferences());
    builder.setIdentifiedBy(basicRecord.getIdentifiedBy());
    builder.setRecordedBy(basicRecord.getRecordedBy());
    builder.setOccurrenceStatus(basicRecord.getOccurrenceStatus());
    builder.setIsClustered(basicRecord.getIsClustered());
    builder.setDatasetID(basicRecord.getDatasetID());
    builder.setDatasetName(basicRecord.getDatasetName());
    builder.setOtherCatalogNumbers(basicRecord.getOtherCatalogNumbers());
    builder.setPreparations(basicRecord.getPreparations());
    builder.setSamplingProtocol(basicRecord.getSamplingProtocol());

    // Agent
    builder.setIdentifiedByIds(JsonConverter.convertAgentList(basicRecord.getIdentifiedByIds()));
    builder.setRecordedByIds(JsonConverter.convertAgentList(basicRecord.getRecordedByIds()));

    // VocabularyConcept
    JsonConverter.convertVocabularyConcept(basicRecord.getLifeStage())
        .ifPresent(builder::setLifeStage);
    JsonConverter.convertVocabularyConcept(basicRecord.getEstablishmentMeans())
        .ifPresent(builder::setEstablishmentMeans);
    JsonConverter.convertVocabularyConcept(basicRecord.getDegreeOfEstablishment())
        .ifPresent(builder::setDegreeOfEstablishment);
    JsonConverter.convertVocabularyConcept(basicRecord.getPathway()).ifPresent(builder::setPathway);

    // License
    JsonConverter.convertLicense(basicRecord.getLicense()).ifPresent(builder::setLicense);

    // Multivalue fields
    JsonConverter.convertToMultivalue(basicRecord.getRecordedBy())
        .ifPresent(builder::setRecordedByJoined);
    JsonConverter.convertToMultivalue(basicRecord.getIdentifiedBy())
        .ifPresent(builder::setIdentifiedByJoined);
    JsonConverter.convertToMultivalue(basicRecord.getPreparations())
        .ifPresent(builder::setPreparationsJoined);
    JsonConverter.convertToMultivalue(basicRecord.getSamplingProtocol())
        .ifPresent(builder::setSamplingProtocolJoined);
    JsonConverter.convertToMultivalue(basicRecord.getOtherCatalogNumbers())
        .ifPresent(builder::setOtherCatalogNumbersJoined);
  }

  private void mapTemporalRecord(OccurrenceJsonRecord.Builder builder) {

    builder.setYear(temporalRecord.getYear());
    builder.setMonth(temporalRecord.getMonth());
    builder.setDay(temporalRecord.getDay());
    builder.setStartDayOfYear(temporalRecord.getStartDayOfYear());
    builder.setEndDayOfYear(temporalRecord.getEndDayOfYear());
    builder.setModified(temporalRecord.getModified());
    builder.setDateIdentified(temporalRecord.getDateIdentified());

    JsonConverter.convertEventDate(temporalRecord.getEventDate()).ifPresent(builder::setEventDate);
    JsonConverter.convertEventDateSingle(temporalRecord).ifPresent(builder::setEventDateSingle);
  }

  private void mapLocationRecord(OccurrenceJsonRecord.Builder builder) {

    builder.setContinent(locationRecord.getContinent());
    builder.setContinent(locationRecord.getContinent());
    builder.setWaterBody(locationRecord.getWaterBody());
    builder.setCountry(locationRecord.getCountry());
    builder.setCountryCode(locationRecord.getCountryCode());
    builder.setPublishingCountry(locationRecord.getPublishingCountry());
    builder.setStateProvince(locationRecord.getStateProvince());
    builder.setMinimumElevationInMeters(locationRecord.getMinimumElevationInMeters());
    builder.setMaximumElevationInMeters(locationRecord.getMaximumElevationInMeters());
    builder.setElevation(locationRecord.getElevation());
    builder.setElevationAccuracy(locationRecord.getElevationAccuracy());
    builder.setMinimumDepthInMeters(locationRecord.getMinimumDepthInMeters());
    builder.setMaximumDepthInMeters(locationRecord.getMaximumDepthInMeters());
    builder.setCoordinateUncertaintyInMeters(locationRecord.getCoordinateUncertaintyInMeters());
    builder.setCoordinatePrecision(locationRecord.getCoordinatePrecision());
    builder.setHasCoordinate(locationRecord.getHasCoordinate());
    builder.setRepatriated(locationRecord.getRepatriated());
    builder.setHasGeospatialIssue(locationRecord.getHasGeospatialIssue());
    builder.setLocality(locationRecord.getLocality());
    builder.setFootprintWKT(locationRecord.getFootprintWKT());

    if (locationRecord.getDecimalLongitude() != null
        && locationRecord.getDecimalLatitude() != null) {
      builder.setDecimalLatitude(locationRecord.getDecimalLatitude());
      builder.setDecimalLongitude(locationRecord.getDecimalLongitude());
      // geo_point
      builder.setCoordinates(
          JsonConverter.convertCoordinates(
              locationRecord.getDecimalLongitude(), locationRecord.getDecimalLatitude()));
      // geo_shape
      builder.setScoordinates(
          JsonConverter.convertScoordinates(
              locationRecord.getDecimalLongitude(), locationRecord.getDecimalLatitude()));
    }

    JsonConverter.convertGadm(locationRecord.getGadm()).ifPresent(builder::setGadm);
  }

  private void mapTaxonRecord(OccurrenceJsonRecord.Builder builder) {

    GbifClassification.Builder gbifClassificationBuilder =
        GbifClassification.newBuilder()
            .setAcceptedUsage(JsonConverter.convertRankedName(taxonRecord.getAcceptedUsage()))
            .setSynonym(taxonRecord.getSynonym())
            .setUsage(JsonConverter.convertRankedName(taxonRecord.getUsage()))
            .setIucnRedListCategoryCode(taxonRecord.getIucnRedListCategoryCode())
            .setClassification(JsonConverter.convertRankedNames(taxonRecord.getClassification()))
            .setTaxonKey(JsonConverter.convertTaxonKey(taxonRecord));

    JsonConverter.convertDiagnostic(taxonRecord.getDiagnostics())
        .ifPresent(gbifClassificationBuilder::setDiagnostics);

    JsonConverter.convertParsedName(taxonRecord.getUsageParsedName())
        .ifPresent(gbifClassificationBuilder::setUsageParsedName);

    JsonConverter.convertGenericName(taxonRecord)
        .ifPresent(
            genereicName -> {
              if (gbifClassificationBuilder.getUsageParsedName() != null) {
                gbifClassificationBuilder.getUsageParsedName().setGenericName(genereicName);
              }
            });

    JsonConverter.convertClassificationPath(taxonRecord)
        .ifPresent(gbifClassificationBuilder::setClassificationPath);

    // Classification
    if (taxonRecord.getClassification() != null) {
      for (RankedName rankedName : taxonRecord.getClassification()) {
        Rank rank = rankedName.getRank();
        switch (rank) {
          case KINGDOM:
            gbifClassificationBuilder.setKingdom(rank.name());
            gbifClassificationBuilder.setKingdomKey(rankedName.getKey());
            break;
          case PHYLUM:
            gbifClassificationBuilder.setPhylum(rank.name());
            gbifClassificationBuilder.setPhylumKey(rankedName.getKey());
            break;
          case CLASS:
            gbifClassificationBuilder.setClass$(rank.name());
            gbifClassificationBuilder.setClassKey(rankedName.getKey());
            break;
          case ORDER:
            gbifClassificationBuilder.setOrder(rank.name());
            gbifClassificationBuilder.setOrderKey(rankedName.getKey());
            break;
          case FAMILY:
            gbifClassificationBuilder.setFamily(rank.name());
            gbifClassificationBuilder.setFamilyKey(rankedName.getKey());
            break;
          case GENUS:
            gbifClassificationBuilder.setGenus(rank.name());
            gbifClassificationBuilder.setGenusKey(rankedName.getKey());
            break;
          case SPECIES:
            gbifClassificationBuilder.setSpecies(rank.name());
            gbifClassificationBuilder.setSpeciesKey(rankedName.getKey());
            break;
          default:
            // NOP
        }
      }
    }

    // Set main GbifClassification
    builder.setGbifClassification(gbifClassificationBuilder.build());
  }

  private void mapGrscicollRecord(OccurrenceJsonRecord.Builder builder) {

    Optional.ofNullable(grscicollRecord.getInstitutionMatch())
        .map(Match::getKey)
        .ifPresent(builder::setInstitutionKey);

    Optional.ofNullable(grscicollRecord.getCollectionMatch())
        .map(Match::getKey)
        .ifPresent(builder::setCollectionKey);
  }

  private void mapMultimediaRecord(OccurrenceJsonRecord.Builder builder) {

    builder.setMultimediaItems(JsonConverter.convertMultimediaList(multimediaRecord));
    builder.setMediaTypes(JsonConverter.convertMultimediaType(multimediaRecord));
    builder.setMediaLicenses(JsonConverter.convertMultimediaLicense(multimediaRecord));
  }

  private void mapExtendedRecord(OccurrenceJsonRecord.Builder builder) {

    builder.setId(extendedRecord.getId());
    builder.setAll(JsonConverter.convertFieldAll(extendedRecord));
    builder.setExtensions(JsonConverter.convertExtenstions(extendedRecord));
    builder.setVerbatim(JsonConverter.convertVerbatimRecord(extendedRecord));

    // Set raw as indexed
    extractOptValue(extendedRecord, DwcTerm.recordNumber).ifPresent(builder::setRecordNumber);
    extractOptValue(extendedRecord, DwcTerm.organismID).ifPresent(builder::setOrganismId);
    extractOptValue(extendedRecord, DwcTerm.eventID).ifPresent(builder::setEventId);
    extractOptValue(extendedRecord, DwcTerm.parentEventID).ifPresent(builder::setParentEventId);
    extractOptValue(extendedRecord, DwcTerm.institutionCode).ifPresent(builder::setInstitutionCode);
    extractOptValue(extendedRecord, DwcTerm.collectionCode).ifPresent(builder::setCollectionCode);
    extractOptValue(extendedRecord, DwcTerm.catalogNumber).ifPresent(builder::setCatalogNumber);
    extractOptValue(extendedRecord, DwcTerm.occurrenceID).ifPresent(builder::setOccurrenceId);
  }

  private void mapIssues(OccurrenceJsonRecord.Builder builder) {
    JsonConverter.mapIssues(
        Arrays.asList(
            metadataRecord,
            basicRecord,
            temporalRecord,
            locationRecord,
            taxonRecord,
            grscicollRecord,
            multimediaRecord),
        builder::setIssuess,
        builder::setNotIssues);
  }

  private void mapCreated(OccurrenceJsonRecord.Builder builder) {
    JsonConverter.getMaxCreationDate(
            metadataRecord,
            basicRecord,
            temporalRecord,
            locationRecord,
            taxonRecord,
            grscicollRecord,
            multimediaRecord)
        .ifPresent(builder::setCreated);
  }
}
