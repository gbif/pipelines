package org.gbif.pipelines.transform.function;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function converts an extended record to an interpreted KeyValue of occurrenceId and Event.
 * This function returns multiple outputs,
 * a. Interpreted version of raw temporal data as KV<String,Event>
 * b. Issues and lineages applied on raw data to get the interpreted result, as KV<String,IssueLineageRecord>
 */
public class LocationTransform extends DoFn<ExtendedRecord, KV<String, Location>> {

  private static final Logger LOG = LoggerFactory.getLogger(LocationTransform.class);
  /**
   * tags for locating different type of outputs send by this function
   */
  private final TupleTag<KV<String, Location>> locationDataTag = new TupleTag<KV<String, Location>>() {};
  private final TupleTag<KV<String, IssueLineageRecord>> locationIssueTag =
    new TupleTag<KV<String, IssueLineageRecord>>() {};

  public TupleTag<KV<String, Location>> getLocationDataTag() {
    return locationDataTag;
  }

  public TupleTag<KV<String, IssueLineageRecord>> getLocationIssueTag() {
    return locationIssueTag;
  }

  @ProcessElement
  public void processElement(ProcessContext ctx) {
    ExtendedRecord record = ctx.element();

    Location location = toLocation(record);

    /*
      Interpreting Country and Country code
    */
    IssueLineageRecord issueLineageRecord = Interpretation.of(record)
      .using(LocationInterpreter.interpretCountry(location))
      .using(LocationInterpreter.interpretCountryCode(location))
      .using(LocationInterpreter.interpretContinent(location))
      .getIssueLineageRecord(record.getId());

    //all issues and lineages are dumped on this object
    issueLineageRecord.setOccurenceId(record.getId());
    LOG.debug("Raw records converted to spatial category reporting issues and lineages");
    ctx.output(locationDataTag, KV.of(location.getOccurrenceID(), location));
    ctx.output(locationIssueTag, KV.of(location.getOccurrenceID(), issueLineageRecord));
  }

  private static Location toLocation(ExtendedRecord record) {
    Function<DwcTerm, String> getValue = term -> record.getCoreTerms().get(term.qualifiedName());
    return Location.newBuilder()
      //mapping raw record with interpreted ones
      .setOccurrenceID(record.getId())
      .setLocationID(getValue.apply(DwcTerm.locationID))
      .setHigherGeographyID(getValue.apply(DwcTerm.higherGeographyID))
      .setHigherGeography(getValue.apply(DwcTerm.higherGeography))
      .setWaterBody(getValue.apply(DwcTerm.waterBody))
      .setIslandGroup(getValue.apply(DwcTerm.islandGroup))
      .setIsland(getValue.apply(DwcTerm.island))
      .setStateProvince(getValue.apply(DwcTerm.stateProvince))
      .setCounty(getValue.apply(DwcTerm.county))
      .setMunicipality(getValue.apply(DwcTerm.municipality))
      .setLocality(getValue.apply(DwcTerm.locality))
      .setVerbatimLocality(getValue.apply(DwcTerm.verbatimLocality))
      .setMinimumElevationInMeters(getValue.apply(DwcTerm.minimumElevationInMeters))
      .setMaximumElevationInMeters(getValue.apply(DwcTerm.maximumElevationInMeters))
      .setVerbatimElevation(getValue.apply(DwcTerm.verbatimElevation))
      .setMaximumDepthInMeters(getValue.apply(DwcTerm.maximumDepthInMeters))
      .setMinimumDepthInMeters(getValue.apply(DwcTerm.minimumDepthInMeters))
      .setLocationAccordingTo(getValue.apply(DwcTerm.locationAccordingTo))
      .setLocationRemarks(getValue.apply(DwcTerm.locationRemarks))
      .setDecimalLatitude(getValue.apply(DwcTerm.decimalLatitude))
      .setDecimalLongitude(getValue.apply(DwcTerm.decimalLongitude))
      .setGeodeticDatum(getValue.apply(DwcTerm.geodeticDatum))
      .setCoordinateUncertaintyInMeters(getValue.apply(DwcTerm.coordinateUncertaintyInMeters))
      .setCoordinatePrecision(getValue.apply(DwcTerm.coordinatePrecision))
      .setPointRadiusSpatialFit(getValue.apply(DwcTerm.pointRadiusSpatialFit))
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
      .build();
  }
}
