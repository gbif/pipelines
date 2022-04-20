package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.util.Arrays;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
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
public class OccurrenceJsonConverter {

  private final MetadataRecord metadata;
  private final GbifIdRecord gbifId;
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

    mapMetadataRecord(builder);
    mapGbifIdRecord(builder);
    mapBasicRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapTaxonRecord(builder);
    mapGrscicollRecord(builder);
    mapMultimediaRecord(builder);
    mapExtendedRecord(builder);

    return builder.build();
  }

  public String toJson() {
    return convert().toString();
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
        .setProjectId(metadata.getProjectId())
        .setProtocol(metadata.getProtocol())
        .setPublisherTitle(metadata.getPublisherTitle())
        .setPublishingOrganizationKey(metadata.getPublishingOrganizationKey());

    JsonConverter.convertToDate(metadata.getLastCrawled()).ifPresent(builder::setLastCrawled);
  }

  private void mapGbifIdRecord(OccurrenceJsonRecord.Builder builder) {
    builder.setGbifId(gbifId.getGbifId());
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
        .setIdentifiedBy(basic.getIdentifiedBy())
        .setRecordedBy(basic.getRecordedBy())
        .setOccurrenceStatus(basic.getOccurrenceStatus())
        .setIsClustered(basic.getIsClustered())
        .setDatasetID(basic.getDatasetID())
        .setDatasetName(basic.getDatasetName())
        .setOtherCatalogNumbers(basic.getOtherCatalogNumbers())
        .setPreparations(basic.getPreparations())
        .setSamplingProtocol(basic.getSamplingProtocol());

    // Agent
    builder
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

  private void mapTaxonRecord(OccurrenceJsonRecord.Builder builder) {

    GbifClassification.Builder classificationBuilder =
        GbifClassification.newBuilder()
            .setSynonym(taxon.getSynonym())
            .setIucnRedListCategoryCode(taxon.getIucnRedListCategoryCode())
            .setClassification(JsonConverter.convertRankedNames(taxon.getClassification()))
            .setTaxonKey(JsonConverter.convertTaxonKey(taxon));

    JsonConverter.convertRankedName(taxon.getUsage()).ifPresent(classificationBuilder::setUsage);

    JsonConverter.convertRankedName(taxon.getAcceptedUsage())
        .ifPresent(classificationBuilder::setAcceptedUsage);

    JsonConverter.convertDiagnostic(taxon.getDiagnostics())
        .ifPresent(classificationBuilder::setDiagnostics);

    JsonConverter.convertParsedName(taxon.getUsageParsedName())
        .ifPresent(classificationBuilder::setUsageParsedName);

    JsonConverter.convertGenericName(taxon)
        .ifPresent(
            genereicName -> {
              if (classificationBuilder.getUsageParsedName() != null) {
                classificationBuilder.getUsageParsedName().setGenericName(genereicName);
              }
            });

    JsonConverter.convertClassificationPath(taxon)
        .ifPresent(classificationBuilder::setClassificationPath);

    // Classification
    if (taxon.getClassification() != null) {
      for (RankedName rankedName : taxon.getClassification()) {
        Rank rank = rankedName.getRank();
        switch (rank) {
          case KINGDOM:
            classificationBuilder.setKingdom(rankedName.getName());
            classificationBuilder.setKingdomKey(rankedName.getKey());
            break;
          case PHYLUM:
            classificationBuilder.setPhylum(rankedName.getName());
            classificationBuilder.setPhylumKey(rankedName.getKey());
            break;
          case CLASS:
            classificationBuilder.setClass$(rankedName.getName());
            classificationBuilder.setClassKey(rankedName.getKey());
            break;
          case ORDER:
            classificationBuilder.setOrder(rankedName.getName());
            classificationBuilder.setOrderKey(rankedName.getKey());
            break;
          case FAMILY:
            classificationBuilder.setFamily(rankedName.getName());
            classificationBuilder.setFamilyKey(rankedName.getKey());
            break;
          case GENUS:
            classificationBuilder.setGenus(rankedName.getName());
            classificationBuilder.setGenusKey(rankedName.getKey());
            break;
          case SPECIES:
            classificationBuilder.setSpecies(rankedName.getName());
            classificationBuilder.setSpeciesKey(rankedName.getKey());
            break;
          default:
            // NOP
        }
      }
    }

    // Raw to index classification
    extractOptValue(verbatim, DwcTerm.taxonID).ifPresent(classificationBuilder::setTaxonID);
    extractOptValue(verbatim, DwcTerm.scientificName)
        .ifPresent(classificationBuilder::setVerbatimScientificName);

    // Set main GbifClassification
    builder.setGbifClassification(classificationBuilder.build());
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
        .setExtensions(JsonConverter.convertExtenstions(verbatim))
        .setVerbatim(JsonConverter.convertVerbatimRecord(verbatim));

    // Set raw as indexed
    extractOptValue(verbatim, DwcTerm.recordNumber).ifPresent(builder::setRecordNumber);
    extractOptValue(verbatim, DwcTerm.organismID).ifPresent(builder::setOrganismId);
    extractOptValue(verbatim, DwcTerm.eventID).ifPresent(builder::setEventId);
    extractOptValue(verbatim, DwcTerm.parentEventID).ifPresent(builder::setParentEventId);
    extractOptValue(verbatim, DwcTerm.institutionCode).ifPresent(builder::setInstitutionCode);
    extractOptValue(verbatim, DwcTerm.collectionCode).ifPresent(builder::setCollectionCode);
    extractOptValue(verbatim, DwcTerm.catalogNumber).ifPresent(builder::setCatalogNumber);
    extractOptValue(verbatim, DwcTerm.occurrenceID).ifPresent(builder::setOccurrenceId);
  }

  private void mapIssues(OccurrenceJsonRecord.Builder builder) {
    JsonConverter.mapIssues(
        Arrays.asList(metadata, gbifId, basic, temporal, location, taxon, grscicoll, multimedia),
        builder::setIssues,
        builder::setNotIssues);
  }

  private void mapCreated(OccurrenceJsonRecord.Builder builder) {
    JsonConverter.getMaxCreationDate(
            metadata, gbifId, basic, temporal, location, taxon, grscicoll, multimedia)
        .ifPresent(builder::setCreated);
  }
}
