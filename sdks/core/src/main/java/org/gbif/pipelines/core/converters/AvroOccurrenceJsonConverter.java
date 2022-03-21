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
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.grscicoll.Match;
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

    mapMetadataRecord(builder);
    mapBasicRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapTaxonRecord(builder);
    mapGrscicollRecord(builder);
    mapMultimediaRecord(builder);
    mapExtendedRecord(builder);
    mapIssues(builder);

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

  private void mapTemporalRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapLocationRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapTaxonRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapGrscicollRecord(OccurrenceJsonRecord.Builder builder) {

    Optional.ofNullable(grscicollRecord.getInstitutionMatch())
        .map(Match::getKey)
        .ifPresent(builder::setInstitutionKey);

    Optional.ofNullable(grscicollRecord.getCollectionMatch())
        .map(Match::getKey)
        .ifPresent(builder::setCollectionKey);
  }

  private void mapMultimediaRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapExtendedRecord(OccurrenceJsonRecord.Builder builder) {

    builder.setId(extendedRecord.getId());
    builder.setAll(JsonConverter.convertAll(extendedRecord));
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
}
