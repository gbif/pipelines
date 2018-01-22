package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ExtendedRecordToLocationTransformer extends DoFn<ExtendedRecord,KV<String,Location>> {
  @ProcessElement
  public void processElement(ProcessContext ctx){
    ExtendedRecord record = ctx.element();
    Location loc = new Location();
    loc.setOccurrenceID(record.getCoreTerms().get(DwcTerm.occurrenceID));
    loc.setLocationID(record.getCoreTerms().get(DwcTerm.locationID));
    loc.setHigherGeographyID(record.getCoreTerms().get(DwcTerm.higherGeographyID));
    loc.setHigherGeography(record.getCoreTerms().get(DwcTerm.higherGeography));
    loc.setContinent(record.getCoreTerms().get(DwcTerm.continent));
    loc.setWaterBody(record.getCoreTerms().get(DwcTerm.waterBody));
    loc.setIslandGroup(record.getCoreTerms().get(DwcTerm.islandGroup));
    loc.setIsland(record.getCoreTerms().get(DwcTerm.island));
    loc.setCountry(record.getCoreTerms().get(DwcTerm.country));
    loc.setCountryCode(record.getCoreTerms().get(DwcTerm.countryCode));
    loc.setStateProvince(record.getCoreTerms().get(DwcTerm.stateProvince));
    loc.setCounty(record.getCoreTerms().get(DwcTerm.county));
    loc.setMunicipality(record.getCoreTerms().get(DwcTerm.municipality));
    loc.setLocality(record.getCoreTerms().get(DwcTerm.locality));
    loc.setVerbatimLocality(record.getCoreTerms().get(DwcTerm.verbatimLocality));
    loc.setMinimumElevationInMeters(record.getCoreTerms().get(DwcTerm.minimumElevationInMeters));
    loc.setMaximumElevationInMeters(record.getCoreTerms().get(DwcTerm.maximumElevationInMeters));
    loc.setVerbatimElevation(record.getCoreTerms().get(DwcTerm.verbatimElevation));
    loc.setMaximumDepthInMeters(record.getCoreTerms().get(DwcTerm.maximumDepthInMeters));
    loc.setMinimumDepthInMeters(record.getCoreTerms().get(DwcTerm.minimumDepthInMeters));
    loc.setLocationAccordingTo(record.getCoreTerms().get(DwcTerm.locationAccordingTo));
    loc.setLocationRemarks(record.getCoreTerms().get(DwcTerm.locationRemarks));
    loc.setDecimalLatitude(record.getCoreTerms().get(DwcTerm.decimalLatitude));
    loc.setDecimalLongitude(record.getCoreTerms().get(DwcTerm.decimalLongitude));
    loc.setGeodeticDatum(record.getCoreTerms().get(DwcTerm.geodeticDatum));
    loc.setCoordinateUncertaintyInMeters(record.getCoreTerms().get(DwcTerm.coordinateUncertaintyInMeters));
    loc.setCoordinatePrecision(record.getCoreTerms().get(DwcTerm.coordinatePrecision));
    loc.setPointRadiusSpatialFit(record.getCoreTerms().get(DwcTerm.pointRadiusSpatialFit));
    loc.setVerbatimCoordinates(record.getCoreTerms().get(DwcTerm.verbatimCoordinates));
    loc.setVerbatimLatitude(record.getCoreTerms().get(DwcTerm.verbatimLatitude));
    loc.setVerbatimLongitude(record.getCoreTerms().get(DwcTerm.verbatimLongitude));
    loc.setVerbatimCoordinateSystem(record.getCoreTerms().get(DwcTerm.verbatimCoordinateSystem));
    loc.setVerbatimSRS(record.getCoreTerms().get(DwcTerm.verbatimSRS));
    loc.setFootprintWKT(record.getCoreTerms().get(DwcTerm.footprintWKT));
    loc.setFootprintSRS(record.getCoreTerms().get(DwcTerm.footprintSRS));
    loc.setFootprintSpatialFit(record.getCoreTerms().get(DwcTerm.footprintSpatialFit));
    loc.setGeoreferencedBy(record.getCoreTerms().get(DwcTerm.georeferencedBy));
    loc.setGeoreferencedDate(record.getCoreTerms().get(DwcTerm.georeferencedDate));
    loc.setGeoreferenceProtocol(record.getCoreTerms().get(DwcTerm.georeferenceProtocol));
    loc.setGeoreferenceSources(record.getCoreTerms().get(DwcTerm.georeferenceSources));
    loc.setGeoreferenceVerificationStatus(record.getCoreTerms().get(DwcTerm.georeferenceVerificationStatus));
    loc.setGeoreferenceRemarks(record.getCoreTerms().get(DwcTerm.georeferenceRemarks));
    loc.setInstitutionID(record.getCoreTerms().get(DwcTerm.institutionID));
    loc.setCollectionID(record.getCoreTerms().get(DwcTerm.collectionID));
    loc.setDatasetID(record.getCoreTerms().get(DwcTerm.datasetID));
    loc.setInstitutionCode(record.getCoreTerms().get(DwcTerm.institutionCode));
    loc.setCollectionCode(record.getCoreTerms().get(DwcTerm.collectionCode));
    loc.setDatasetName(record.getCoreTerms().get(DwcTerm.datasetName));
    loc.setOwnerInstitutionCode(record.getCoreTerms().get(DwcTerm.ownerInstitutionCode));
    loc.setDynamicProperties(record.getCoreTerms().get(DwcTerm.dynamicProperties));
    loc.setInformationWithheld(record.getCoreTerms().get(DwcTerm.informationWithheld));
    loc.setDataGeneralizations(record.getCoreTerms().get(DwcTerm.dataGeneralizations));
    ctx.output(KV.of(loc.getOccurrenceID().toString(),loc));
  }
}
