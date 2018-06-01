package org.gbif.pipelines.mapper;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.record.ExtendedRecord;
import org.gbif.pipelines.io.avro.record.location.LocationRecord;

import java.util.function.Function;

public class LocationRecordMapper {

  private LocationRecordMapper() {
    //Can't have an instance
  }

  /**
   * Base fields mapping
   */
  public static LocationRecord map(ExtendedRecord record) {
    Function<DwcTerm, String> getValue = dwcterm -> record.getCoreTerms().get(dwcterm.qualifiedName());
    return LocationRecord.newBuilder()
      .setId(record.getId())
      .setLocationID(getValue.apply(DwcTerm.locationID))
      .setHigherGeographyID(getValue.apply(DwcTerm.higherGeographyID))
      .setHigherGeography(getValue.apply(DwcTerm.higherGeography))
      .setIslandGroup(getValue.apply(DwcTerm.islandGroup))
      .setIsland(getValue.apply(DwcTerm.island))
      .setCounty(getValue.apply(DwcTerm.county))
      .setMunicipality(getValue.apply(DwcTerm.municipality))
      .setLocality(getValue.apply(DwcTerm.locality))
      .setVerbatimLocality(getValue.apply(DwcTerm.verbatimLocality))
      .setVerbatimElevation(getValue.apply(DwcTerm.verbatimElevation))
      .setLocationAccordingTo(getValue.apply(DwcTerm.locationAccordingTo))
      .setLocationRemarks(getValue.apply(DwcTerm.locationRemarks))
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
      .setPointRadiusSpatialFit(getValue.apply(DwcTerm.pointRadiusSpatialFit))
      .setContinent(getValue.apply(DwcTerm.continent))
      .setWaterBody(getValue.apply(DwcTerm.waterBody))
      .setCountry(getValue.apply(DwcTerm.country))
      .setCountryCode(getValue.apply(DwcTerm.countryCode))
      .setStateProvince(getValue.apply(DwcTerm.stateProvince))
      .setVerbatimDepth(getValue.apply(DwcTerm.verbatimDepth))
      .build();
  }

}
