package au.org.ala.pipelines.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.pipelines.core.converters.JsonConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.*;

/** Converts AVRO occurrence record artefacts to a JSON for indexing. */
@Slf4j
@Builder
public class ALAOccurrenceJsonConverter {

  private final ALAMetadataRecord metadata;
  private final ALAUUIDRecord uuid;
  private final BasicRecord basic;
  private final TemporalRecord temporal;
  private final LocationRecord location;
  private final ALATaxonRecord taxon;
  private final MultimediaRecord multimedia;
  private final ExtendedRecord verbatim;
  private final EventCoreRecord eventCore;

  private final EventInheritedRecord eventInheritedRecord;
  private final LocationInheritedRecord locationInheritedRecord;
  private final TemporalInheritedRecord temporalInheritedRecord;

  private final MeasurementOrFactRecord measurementOrFact;

  private final ALASensitivityRecord sensitivityRecord;

  public OccurrenceJsonRecord convert() {

    OccurrenceJsonRecord.Builder builder = OccurrenceJsonRecord.newBuilder();
    builder.setId(uuid.getUuid());
    builder.setCreated(uuid.getFirstLoaded().toString());
    builder.setGbifId(0);
    mapMetadataRecord(builder);
    mapBasicRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapTaxonRecord(builder);
    mapMultimediaRecord(builder);
    mapExtendedRecord(builder);
    mapMeasurementOrFactRecord(builder);
    mapInherited(builder);

    // synthesize a locationID if one isnt provided
    if (builder.getLocationID() == null
        && location.getDecimalLatitude() != null
        && location.getDecimalLongitude() != null) {
      builder.setLocationID(
          Math.abs(location.getDecimalLatitude())
              + (location.getDecimalLatitude() > 0 ? "N" : "S")
              + ", "
              + Math.abs(location.getDecimalLongitude())
              + (location.getDecimalLongitude() > 0 ? "E" : "W"));
    }

    return builder.build();
  }

  public String toJson() {
    return convert().toString();
  }

  private void mapMetadataRecord(OccurrenceJsonRecord.Builder builder) {
    builder
        .setDatasetKey(metadata.getDataResourceUid())
        .setDatasetTitle(metadata.getDataResourceName());
  }

  private void mapInherited(OccurrenceJsonRecord.Builder builder) {

    boolean hasCoordsInfo = builder.getDecimalLatitude() != null;
    boolean hasCountryInfo = builder.getCountryCode() != null;
    boolean hasStateInfo = builder.getStateProvince() != null;
    boolean hasYearInfo = builder.getYear() != null;
    boolean hasMonthInfo = builder.getMonth() != null;
    boolean hasLocationID = builder.getLocationID() != null;

    // extract location & temporal information from
    if (!hasYearInfo && temporalInheritedRecord.getYear() != null) {
      builder.setYear(temporalInheritedRecord.getYear());
    }

    if (!hasMonthInfo && temporalInheritedRecord.getMonth() != null) {
      builder.setMonth(temporalInheritedRecord.getMonth());
    }

    if (!hasCountryInfo && locationInheritedRecord.getCountryCode() != null) {
      builder.setCountryCode(locationInheritedRecord.getCountryCode());
    }

    if (!hasStateInfo && locationInheritedRecord.getStateProvince() != null) {
      builder.setStateProvince(locationInheritedRecord.getStateProvince());
    }

    if (!hasCoordsInfo
        && locationInheritedRecord.getDecimalLatitude() != null
        && locationInheritedRecord.getDecimalLongitude() != null) {
      builder
          .setHasCoordinate(true)
          .setDecimalLatitude(locationInheritedRecord.getDecimalLatitude())
          .setDecimalLongitude(locationInheritedRecord.getDecimalLongitude())
          // geo_point
          .setCoordinates(
              JsonConverter.convertCoordinates(
                  locationInheritedRecord.getDecimalLongitude(),
                  locationInheritedRecord.getDecimalLatitude()))
          // geo_shape
          .setScoordinates(
              JsonConverter.convertScoordinates(
                  locationInheritedRecord.getDecimalLongitude(),
                  locationInheritedRecord.getDecimalLatitude()));
    }

    if (!hasLocationID && eventInheritedRecord.getLocationID() != null) {
      builder.setLocationID(eventInheritedRecord.getLocationID());
    }

    if (eventCore != null
        && eventCore.getParentsLineage() != null
        && !eventCore.getParentsLineage().isEmpty()) {

      List<String> eventIDs =
          eventCore.getParentsLineage().stream()
              .sorted(
                  Comparator.comparingInt(org.gbif.pipelines.io.avro.Parent::getOrder).reversed())
              .map(e -> e.getId())
              .collect(Collectors.toList());
      eventIDs.add(eventCore.getId());

      List<String> eventTypes =
          eventCore.getParentsLineage().stream()
              .sorted(
                  Comparator.comparingInt(org.gbif.pipelines.io.avro.Parent::getOrder).reversed())
              .map(e -> e.getEventType())
              .collect(Collectors.toList());

      if (eventCore.getEventType() != null) {
        eventTypes.add(eventCore.getEventType().getConcept());
      } else {
        String rawEventType = verbatim.getCoreTerms().get(GbifTerm.eventType.qualifiedName());
        if (rawEventType != null) {
          eventTypes.add(rawEventType);
        }
      }

      // add the eventID / eventy
      builder.setEventTypeHierarchy(eventTypes);
      builder.setEventTypeHierarchyJoined(String.join(" / ", eventTypes));

      builder.setEventHierarchy(eventIDs);
      builder.setEventHierarchyJoined(String.join(" / ", eventIDs));
      builder.setEventHierarchyLevels(eventIDs.size());

    } else {
      // add the eventID and parentEventID to hierarchy for consistency
      List<String> eventHierarchy = new ArrayList<>();
      if (builder.getParentEventId() != null) {
        eventHierarchy.add(builder.getParentEventId());
      }
      if (builder.getEventId() != null) {
        eventHierarchy.add(builder.getEventId());
      }
      builder.setEventHierarchy(eventHierarchy);

      // add the single type to hierarchy for consistency
      List<String> eventTypeHierarchy = new ArrayList<>();
      eventTypeHierarchy.add(builder.getBasisOfRecord());
      builder.setEventTypeHierarchy(eventTypeHierarchy);
    }
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

    // Multi-value fields
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
    // Set  GbifClassification
    GbifClassification gc = convertClassification(verbatim, taxon);

    List<Taxonomy> taxonomy = new ArrayList<>();

    taxonomy.add(
        Taxonomy.newBuilder().setName(gc.getKingdom()).setTaxonKey(gc.getKingdomKey()).build());
    taxonomy.add(
        Taxonomy.newBuilder().setName(gc.getPhylum()).setTaxonKey(gc.getPhylumKey()).build());
    taxonomy.add(
        Taxonomy.newBuilder().setName(gc.getClass$()).setTaxonKey(gc.getClassKey()).build());
    taxonomy.add(
        Taxonomy.newBuilder().setName(gc.getOrder()).setTaxonKey(gc.getOrderKey()).build());
    taxonomy.add(
        Taxonomy.newBuilder().setName(gc.getFamily()).setTaxonKey(gc.getFamilyKey()).build());
    taxonomy.add(
        Taxonomy.newBuilder().setName(gc.getGenus()).setTaxonKey(gc.getGenusKey()).build());
    taxonomy.add(
        Taxonomy.newBuilder().setName(gc.getSpecies()).setTaxonKey(gc.getSpeciesKey()).build());
    if (gc.getAcceptedUsage() != null
        && (gc.getAcceptedUsage().getGuid() != null || gc.getAcceptedUsage().getKey() != null)) {
      taxonomy.add(
          Taxonomy.newBuilder()
              .setName(gc.getAcceptedUsage().getName())
              .setTaxonKey(
                  gc.getAcceptedUsage().getGuid() != null
                      ? gc.getAcceptedUsage().getGuid()
                      : gc.getAcceptedUsage().getKey().toString())
              .build());
    }

    taxonomy =
        taxonomy.stream().filter(tr -> tr.getTaxonKey() != null).collect(Collectors.toList());

    // set taxonomy
    builder.setTaxonomy(taxonomy);
    gc.setTaxonKey(taxonomy.stream().map(t -> t.getTaxonKey()).collect(Collectors.toList()));
    builder.setGbifClassification(gc);
  }

  public static GbifClassification convertClassification(
      ExtendedRecord verbatim, ALATaxonRecord taxon) {

    GbifClassification.Builder classificationBuilder =
        GbifClassification.newBuilder().setTaxonID(taxon.getTaxonConceptID());

    classificationBuilder.setAcceptedUsage(
        org.gbif.pipelines.io.avro.json.RankedName.newBuilder()
            .setKey(taxon.getLft())
            .setGuid(taxon.getTaxonConceptID())
            .setName(taxon.getScientificName())
            .setRank(taxon.getTaxonRank())
            .build());

    classificationBuilder.setKingdom(taxon.getKingdom());
    classificationBuilder.setKingdomKey(taxon.getKingdomID());
    classificationBuilder.setPhylum(taxon.getPhylum());
    classificationBuilder.setPhylumKey(taxon.getPhylumID());
    classificationBuilder.setClass$(taxon.getClasss());
    classificationBuilder.setClassKey(taxon.getClassID());
    classificationBuilder.setOrder(taxon.getOrder());
    classificationBuilder.setOrderKey(taxon.getOrderID());
    classificationBuilder.setFamily(taxon.getFamily());
    classificationBuilder.setFamilyKey(taxon.getFamilyID());
    classificationBuilder.setGenus(taxon.getGenus());
    classificationBuilder.setGenusKey(taxon.getGenusID());
    classificationBuilder.setSpecies(taxon.getSpecies());
    classificationBuilder.setSpeciesKey(taxon.getSpeciesID());

    // Raw to index classification
    extractOptValue(verbatim, DwcTerm.taxonID).ifPresent(classificationBuilder::setTaxonID);
    extractOptValue(verbatim, DwcTerm.scientificName)
        .ifPresent(classificationBuilder::setVerbatimScientificName);

    return classificationBuilder.build();
  }

  private void mapMultimediaRecord(OccurrenceJsonRecord.Builder builder) {}

  private void mapExtendedRecord(OccurrenceJsonRecord.Builder builder) {

    builder
        .setAll(JsonConverter.convertFieldAll(verbatim))
        .setExtensions(JsonConverter.convertExtensions(verbatim))
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

  private void mapMeasurementOrFactRecord(OccurrenceJsonRecord.Builder builder) {

    if (measurementOrFact != null) {
      builder.setMeasurementOrFactMethods(
          measurementOrFact.getMeasurementOrFactItems().stream()
              .map(MeasurementOrFact::getMeasurementMethod)
              .filter(x -> StringUtils.isNotEmpty(x))
              .distinct()
              .collect(Collectors.toList()));
      builder.setMeasurementOrFactTypes(
          measurementOrFact.getMeasurementOrFactItems().stream()
              .map(MeasurementOrFact::getMeasurementType)
              .filter(x -> StringUtils.isNotEmpty(x))
              .distinct()
              .collect(Collectors.toList()));

      List<MeasurementOrFactJsonRecord> mofs =
          measurementOrFact.getMeasurementOrFactItems().stream()
              .map(
                  mor -> {
                    return MeasurementOrFactJsonRecord.newBuilder()
                        .setMeasurementID(mor.getMeasurementID())
                        .setMeasurementMethod(mor.getMeasurementMethod())
                        .setMeasurementType(mor.getMeasurementType())
                        .setMeasurementValue(mor.getMeasurementValue())
                        .setMeasurementUnit(mor.getMeasurementUnit())
                        .build();
                  })
              .collect(Collectors.toList());
      builder.setMeasurementOrFacts(mofs);
    }
  }
}
