package org.gbif.pipelines.labs.mapper;

import org.gbif.pipelines.io.avro.ExtendedOccurrence;
import org.gbif.pipelines.io.avro.ExtendedOccurrence.Builder;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.util.Optional;

public class ExtendedOccurrenceMapper {

  private ExtendedOccurrenceMapper() {
    // Can't have an instance
  }

  // TODO: Fill all fields
  public static ExtendedOccurrence map(InterpretedExtendedRecord record, LocationRecord location, TemporalRecord temporal,
      TaxonRecord taxon, MultimediaRecord multimedia) {

    ExtendedOccurrence.Builder builder = ExtendedOccurrence.newBuilder();

    fillCommon(builder, record);
    fillLocation(builder, location);
    fillMultimedia(builder, multimedia);
    fillTaxon(builder, taxon);
    fillTemporal(builder, temporal);

    return builder.build();
  }

  private static void fillTemporal(Builder builder, TemporalRecord temporal){
    builder.setDay(temporal.getDay())
      .setMonth(temporal.getMonth())
      .setYear(temporal.getYear())
      .setEventDate(temporal.getEventDate())
      .setDateIdentified(temporal.getDateIdentified());

    Optional.ofNullable(temporal.getEventTime()).ifPresent(x->builder.setEventTime(x.toString()));
  }

  private static void fillLocation(Builder builder, LocationRecord location){
    builder.setDecimalLatitude(location.getDecimalLatitude())
      .setDecimalLongitude(location.getDecimalLongitude())
      .setCountry(location.getCountry())
      .setCountryCode(location.getCountryCode())
      .setContinent(location.getContinent())
      .setWaterBody(location.getWaterBody())
      .setMinimumElevationInMeters(location.getMinimumElevationInMeters())
      .setMaximumElevationInMeters(location.getMaximumElevationInMeters())
      .setMinimumDepthInMeters(location.getMinimumDepthInMeters())
      .setMaximumDepthInMeters(location.getMaximumDepthInMeters())
      .setMinimumDistanceAboveSurfaceInMeters(location.getMinimumDistanceAboveSurfaceInMeters())
      .setMaximumDistanceAboveSurfaceInMeters(location.getMaximumDistanceAboveSurfaceInMeters())
      .setCoordinatePrecision(location.getCoordinatePrecision())
      .setCoordinateUncertaintyInMeters(location.getCoordinateUncertaintyInMeters())
      .setVerbatimCoordinates(location.getVerbatimCoordinates())
      .setVerbatimLocality(location.getVerbatimLocality())
      .setLocationID(location.getLocationID())
      .setHigherGeography(location.getHigherGeography())
      .setHigherGeographyID(location.getHigherGeographyID())
      .setIsland(location.getIsland())
      .setIslandGroup(location.getIslandGroup())
      .setStateProvince(location.getStateProvince())
      .setCounty(location.getCounty())
      .setMunicipality(location.getMunicipality())
      .setLocality(location.getLocality())
      .setVerbatimElevation(location.getVerbatimElevation())
      .setVerbatimDepth(location.getVerbatimDepth())
      .setLocationAccordingTo(location.getLocationAccordingTo())
      .setLocationRemarks(location.getLocationRemarks())
      .setGeodeticDatum(location.getGeodeticDatum())
      .setVerbatimLatitude(location.getVerbatimLatitude())
      .setVerbatimLongitude(location.getVerbatimLongitude())
      .setPointRadiusSpatialFit(location.getPointRadiusSpatialFit())
      .setVerbatimCoordinateSystem(location.getVerbatimCoordinateSystem())
      .setVerbatimSRS(location.getVerbatimSRS())
      .setFootprintWKT(location.getFootprintWKT())
      .setFootprintSRS(location.getFootprintSRS())
      .setFootprintSpatialFit(location.getFootprintSpatialFit())
      .setGeoreferencedBy(location.getGeoreferencedBy())
      .setGeoreferencedDate(location.getGeoreferencedDate())
      .setGeoreferenceProtocol(location.getGeoreferenceProtocol())
      .setGeoreferenceSources(location.getGeoreferenceSources())
      .setGeoreferenceVerificationStatus(location.getGeoreferenceVerificationStatus())
      .setGeoreferenceRemarks(location.getGeoreferenceRemarks())
      .setDctermsType(location.getDctermsType())
      .setDctermsModified(location.getDctermsModified())
      .setDctermsLanguage(location.getDctermsLanguage())
      .setDctermsLicense(location.getDctermsLicense())
      .setDctermsRightsHolder(location.getDctermsRightsHolder())
      .setDctermsAccessRights(location.getDctermsAccessRights())
      .setDctermsBibliographicCitation(location.getDctermsBibliographicCitation())
      .setInstitutionCode(location.getInstitutionCode())
      .setInstitutionID(location.getInstitutionID())
      .setCollectionID(location.getCollectionID())
      .setDatasetID(location.getDatasetID())
      .setCollectionCode(location.getCollectionCode())
      .setDatasetName(location.getDatasetName())
      .setOwnerInstitutionCode(location.getOwnerInstitutionCode())
      .setInformationWithheld(location.getInformationWithheld())
      .setDataGeneralizations(location.getDataGeneralizations())
      .setDynamicProperties(location.getDynamicProperties());
  }

  private static void fillCommon(Builder builder, InterpretedExtendedRecord record){
    builder.setOccurrenceID(record.getId())
      .setTypeStatus(record.getTypeStatus())
      .setEstablishmentMeans(record.getEstablishmentMeans())
      .setLifeStage(record.getLifeStage())
      .setSex(record.getSex())
      .setBasisOfRecord(record.getBasisOfRecord())
      .setDctermsReferences(record.getReferences());

      Optional.ofNullable(record.getIndividualCount()).ifPresent(x->builder.setIndividualCount(x.toString()));

  }

  private static void fillTaxon(Builder builder, TaxonRecord taxon){
    // TODO: PARSE FIELDS
  }

  private static void fillMultimedia(Builder builder, MultimediaRecord multimedia){
    // TODO: NO FIELDS
  }

}
