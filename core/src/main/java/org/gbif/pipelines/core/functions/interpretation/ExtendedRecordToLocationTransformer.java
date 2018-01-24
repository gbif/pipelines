package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.ContinentParser;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ExtendedRecordToLocationTransformer extends DoFn<ExtendedRecord,KV<String,Location>> {
  @ProcessElement
  public void processElement(ProcessContext ctx){
    ExtendedRecord record = ctx.element();
    Location loc = new Location();
    loc.setOccurrenceID(record.getId());
    loc.setLocationID(record.getCoreTerms().get(DwCATermIdentifier.locationID.getIdentifier()));
    loc.setHigherGeographyID(record.getCoreTerms().get(DwCATermIdentifier.higherGeographyID.getIdentifier()));
    loc.setHigherGeography(record.getCoreTerms().get(DwCATermIdentifier.higherGeography.getIdentifier()));

    /**
     * Interpreting Continent
     */
    CharSequence rawContinent=record.getCoreTerms().get(DwCATermIdentifier.continent.getIdentifier());
    String interpretedContinent = null;
    if(rawContinent!=null)
      interpretedContinent  = ContinentParser.getInstance().parse(rawContinent.toString()).toString();
    loc.setContinent(interpretedContinent);

    loc.setWaterBody(record.getCoreTerms().get(DwCATermIdentifier.waterBody.getIdentifier()));
    loc.setIslandGroup(record.getCoreTerms().get(DwCATermIdentifier.islandGroup.getIdentifier()));
    loc.setIsland(record.getCoreTerms().get(DwCATermIdentifier.island.getIdentifier()));

    /**
     * Interpreting Country code
     */
    CharSequence rawCountry = record.getCoreTerms().get(DwCATermIdentifier.country.getIdentifier());
    CharSequence rawCountryCode =record.getCoreTerms().get(DwCATermIdentifier.countryCode.getIdentifier());
    String interpretedCountry = null; String interpretedCountryCode=null;
    if(rawCountry!=null){
      ParseResult<Country> parseCountry = CountryParser.getInstance().parse(rawCountry.toString().trim());
      if(parseCountry.isSuccessful()) {
        interpretedCountry = parseCountry.getPayload().getTitle();
        interpretedCountryCode = parseCountry.getPayload().getIso3LetterCode();
      }
    }else if (rawCountryCode!=null) {
      ParseResult<Country> parseCountry = CountryParser.getInstance().parse(rawCountryCode.toString().trim());
      if(parseCountry.isSuccessful()) {
        interpretedCountry = parseCountry.getPayload().getTitle();
        interpretedCountryCode = parseCountry.getPayload().getIso3LetterCode();
      }
    }
    loc.setCountry(interpretedCountry);
    loc.setCountryCode(interpretedCountryCode);


    loc.setStateProvince(record.getCoreTerms().get(DwCATermIdentifier.stateProvince.getIdentifier()));
    loc.setCounty(record.getCoreTerms().get(DwCATermIdentifier.county.getIdentifier()));
    loc.setMunicipality(record.getCoreTerms().get(DwCATermIdentifier.municipality.getIdentifier()));
    loc.setLocality(record.getCoreTerms().get(DwCATermIdentifier.locality.getIdentifier()));
    loc.setVerbatimLocality(record.getCoreTerms().get(DwCATermIdentifier.verbatimLocality.getIdentifier()));
    loc.setMinimumElevationInMeters(record.getCoreTerms().get(DwCATermIdentifier.minimumElevationInMeters.getIdentifier()));
    loc.setMaximumElevationInMeters(record.getCoreTerms().get(DwCATermIdentifier.maximumElevationInMeters.getIdentifier()));
    loc.setVerbatimElevation(record.getCoreTerms().get(DwCATermIdentifier.verbatimElevation.getIdentifier()));
    loc.setMaximumDepthInMeters(record.getCoreTerms().get(DwCATermIdentifier.maximumDepthInMeters.getIdentifier()));
    loc.setMinimumDepthInMeters(record.getCoreTerms().get(DwCATermIdentifier.minimumDepthInMeters.getIdentifier()));
    loc.setLocationAccordingTo(record.getCoreTerms().get(DwCATermIdentifier.locationAccordingTo.getIdentifier()));
    loc.setLocationRemarks(record.getCoreTerms().get(DwCATermIdentifier.locationRemarks.getIdentifier()));
    loc.setDecimalLatitude(record.getCoreTerms().get(DwCATermIdentifier.decimalLatitude.getIdentifier()));
    loc.setDecimalLongitude(record.getCoreTerms().get(DwCATermIdentifier.decimalLongitude.getIdentifier()));
    loc.setGeodeticDatum(record.getCoreTerms().get(DwCATermIdentifier.geodeticDatum.getIdentifier()));
    loc.setCoordinateUncertaintyInMeters(record.getCoreTerms().get(DwCATermIdentifier.coordinateUncertaintyInMeters.getIdentifier()));
    loc.setCoordinatePrecision(record.getCoreTerms().get(DwCATermIdentifier.coordinatePrecision.getIdentifier()));
    loc.setPointRadiusSpatialFit(record.getCoreTerms().get(DwCATermIdentifier.pointRadiusSpatialFit.getIdentifier()));
    loc.setVerbatimCoordinates(record.getCoreTerms().get(DwCATermIdentifier.verbatimCoordinates.getIdentifier()));
    loc.setVerbatimLatitude(record.getCoreTerms().get(DwCATermIdentifier.verbatimLatitude.getIdentifier()));
    loc.setVerbatimLongitude(record.getCoreTerms().get(DwCATermIdentifier.verbatimLongitude.getIdentifier()));
    loc.setVerbatimCoordinateSystem(record.getCoreTerms().get(DwCATermIdentifier.verbatimCoordinateSystem.getIdentifier()));
    loc.setVerbatimSRS(record.getCoreTerms().get(DwCATermIdentifier.verbatimSRS.getIdentifier()));
    loc.setFootprintWKT(record.getCoreTerms().get(DwCATermIdentifier.footprintWKT.getIdentifier()));
    loc.setFootprintSRS(record.getCoreTerms().get(DwCATermIdentifier.footprintSRS.getIdentifier()));
    loc.setFootprintSpatialFit(record.getCoreTerms().get(DwCATermIdentifier.footprintSpatialFit.getIdentifier()));
    loc.setGeoreferencedBy(record.getCoreTerms().get(DwCATermIdentifier.georeferencedBy.getIdentifier()));
    loc.setGeoreferencedDate(record.getCoreTerms().get(DwCATermIdentifier.georeferencedDate.getIdentifier()));
    loc.setGeoreferenceProtocol(record.getCoreTerms().get(DwCATermIdentifier.georeferenceProtocol.getIdentifier()));
    loc.setGeoreferenceSources(record.getCoreTerms().get(DwCATermIdentifier.georeferenceSources.getIdentifier()));
    loc.setGeoreferenceVerificationStatus(record.getCoreTerms().get(DwCATermIdentifier.georeferenceVerificationStatus.getIdentifier()));
    loc.setGeoreferenceRemarks(record.getCoreTerms().get(DwCATermIdentifier.georeferenceRemarks.getIdentifier()));
    loc.setInstitutionID(record.getCoreTerms().get(DwCATermIdentifier.institutionID.getIdentifier()));
    loc.setCollectionID(record.getCoreTerms().get(DwCATermIdentifier.collectionID.getIdentifier()));
    loc.setDatasetID(record.getCoreTerms().get(DwCATermIdentifier.datasetID.getIdentifier()));
    loc.setInstitutionCode(record.getCoreTerms().get(DwCATermIdentifier.institutionCode.getIdentifier()));
    loc.setCollectionCode(record.getCoreTerms().get(DwCATermIdentifier.collectionCode.getIdentifier()));
    loc.setDatasetName(record.getCoreTerms().get(DwCATermIdentifier.datasetName.getIdentifier()));
    loc.setOwnerInstitutionCode(record.getCoreTerms().get(DwCATermIdentifier.ownerInstitutionCode.getIdentifier()));
    loc.setDynamicProperties(record.getCoreTerms().get(DwCATermIdentifier.dynamicProperties.getIdentifier()));
    loc.setInformationWithheld(record.getCoreTerms().get(DwCATermIdentifier.informationWithheld.getIdentifier()));
    loc.setDataGeneralizations(record.getCoreTerms().get(DwCATermIdentifier.dataGeneralizations.getIdentifier()));
    ctx.output(KV.of(loc.getOccurrenceID().toString(),loc));
  }
}
