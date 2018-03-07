package org.gbif.pipelines.mapper;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Location;

import java.util.function.Function;

public class LocationMapper {

  private LocationMapper() {
    //Can't have an instance
  }

  public static Location map(ExtendedRecord record) {
    Function<DwcTerm, String> getValue = dwcterm -> record.getCoreTerms().get(dwcterm.qualifiedName());
    return Location.newBuilder()
      .setOccurrenceID(record.getId())
      .setLocationID(getValue.apply(DwcTerm.locationID))
      .setHigherGeographyID(getValue.apply(DwcTerm.higherGeographyID))
      .setHigherGeography(getValue.apply(DwcTerm.higherGeography))
      .setIslandGroup(getValue.apply(DwcTerm.islandGroup))
      .setIsland(getValue.apply(DwcTerm.island))
      .setStateProvince(getValue.apply(DwcTerm.stateProvince))
      .setCounty(getValue.apply(DwcTerm.county))
      .setMunicipality(getValue.apply(DwcTerm.municipality))
      .setLocality(getValue.apply(DwcTerm.locality))
      .setVerbatimLocality(getValue.apply(DwcTerm.verbatimLocality))
      .setVerbatimElevation(getValue.apply(DwcTerm.verbatimElevation))
      .setLocationAccordingTo(getValue.apply(DwcTerm.locationAccordingTo))
      .setLocationRemarks(getValue.apply(DwcTerm.locationRemarks))
      .setDecimalLatitude(getValue.apply(DwcTerm.decimalLatitude))
      .setDecimalLongitude(getValue.apply(DwcTerm.decimalLongitude))
      .setGeodeticDatum(getValue.apply(DwcTerm.geodeticDatum))
      .setVerbatimCoordinates(getValue.apply(DwcTerm.verbatimCoordinates))
      .setVerbatimLatitude(getValue.apply(DwcTerm.verbatimLatitude))
      .setVerbatimLongitude(getValue.apply(DwcTerm.verbatimLongitude))
      .setVerbatimCoordinateSystem(getValue.apply(DwcTerm.verbatimCoordinateSystem))
      .setVerbatimSRS(getValue.apply(DwcTerm.verbatimSRS))
      .setFootprintWKT(getValue.apply(DwcTerm.footprintWKT))
      .setFootprintSRS(getValue.apply(DwcTerm.footprintSRS))
      .setFootprintSpatialFit(getValue.apply(DwcTerm.footprintSpatialFit))
      .setGeoreferencedBy(getValue.apply(DwcTerm.georeferencedBy))
      .setGeoreferencedDate(getValue.apply(DwcTerm.georeferencedDate))
      .setGeoreferenceProtocol(getValue.apply(DwcTerm.georeferenceProtocol))
      .setGeoreferenceSources(getValue.apply(DwcTerm.georeferenceSources))
      .setGeoreferenceVerificationStatus(getValue.apply(DwcTerm.georeferenceVerificationStatus))
      .setGeoreferenceRemarks(getValue.apply(DwcTerm.georeferenceRemarks))
      .setInstitutionID(getValue.apply(DwcTerm.institutionID))
      .setCollectionID(getValue.apply(DwcTerm.collectionID))
      .setDatasetID(getValue.apply(DwcTerm.datasetID))
      .setInstitutionCode(getValue.apply(DwcTerm.institutionCode))
      .setCollectionCode(getValue.apply(DwcTerm.collectionCode))
      .setDatasetName(getValue.apply(DwcTerm.datasetName))
      .setOwnerInstitutionCode(getValue.apply(DwcTerm.ownerInstitutionCode))
      .setDynamicProperties(getValue.apply(DwcTerm.dynamicProperties))
      .setInformationWithheld(getValue.apply(DwcTerm.informationWithheld))
      .setDataGeneralizations(getValue.apply(DwcTerm.dataGeneralizations))
      .setPointRadiusSpatialFit(getValue.apply(DwcTerm.pointRadiusSpatialFit))
      .build();
  }

}
