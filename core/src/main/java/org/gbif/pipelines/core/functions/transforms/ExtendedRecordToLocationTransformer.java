package org.gbif.pipelines.core.functions.transforms;

import org.gbif.api.vocabulary.Continent;
import org.gbif.common.parsers.ContinentParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.CountryCodeInterpreter;
import org.gbif.pipelines.core.functions.interpretation.CountryInterpreter;
import org.gbif.pipelines.core.functions.interpretation.InterpretationException;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * This function converts an extended record to an interpreted KeyValue of occurrenceId and Event.
 * This function returns multiple outputs,
 * a. Interpreted version of raw temporal data as KV<String,Event>
 * b. Issues and lineages applied on raw data to get the interpreted result, as KV<String,IssueLineageRecord>
 */
public class ExtendedRecordToLocationTransformer extends DoFn<ExtendedRecord, KV<String, Location>> {

  /**
   * tags for locating different type of outputs send by this function
   */
  public static final TupleTag<KV<String, Location>> LOCATION_DATA_TAG = new TupleTag<>();
  public static final TupleTag<KV<String, IssueLineageRecord>> LOCATION_ISSUE_TAG =
    new TupleTag<>();

  @ProcessElement
  public void processElement(ProcessContext ctx) {
    ExtendedRecord record = ctx.element();
    Location loc = new Location();

    Map<CharSequence, List<Issue>> fieldIssueMap = new HashMap<>();
    Map<CharSequence, List<Lineage>> fieldLineageMap = new HashMap<>();
    //mapping raw record with interpreted ones
    loc.setOccurrenceID(record.getId());
    loc.setLocationID(record.getCoreTerms().get(DwCATermIdentifier.locationID.getIdentifier()));
    loc.setHigherGeographyID(record.getCoreTerms().get(DwCATermIdentifier.higherGeographyID.getIdentifier()));
    loc.setHigherGeography(record.getCoreTerms().get(DwCATermIdentifier.higherGeography.getIdentifier()));

    /*
      Interpreting Continent
     */

    CharSequence rawContinent = record.getCoreTerms().get(DwCATermIdentifier.continent.getIdentifier());

    if (rawContinent != null) {
      final ParseResult<Continent> parse = ContinentParser.getInstance().parse(rawContinent.toString());
      if (parse.isSuccessful()) {
        loc.setContinent(parse.getPayload().getTitle());
      } else {
        loc.setContinent(null);
        fieldIssueMap.put(DwCATermIdentifier.continent.name(),
                          Collections.singletonList(Issue.newBuilder()
                                                      .setRemark("Could not parse continent because " + (parse.getError()!=null?
                                                        parse.getError().getMessage():" is null"))
                                                      .setIssueType(IssueType.PARSE_ERROR)
                                                      .build()));
        fieldLineageMap.put(DwCATermIdentifier.continent.name(),
                            Collections.singletonList(Lineage.newBuilder()
                                                        .setRemark(
                                                          "Could not parse the continent or invalid value setting it to null")
                                                        .setLineageType(LineageType.SET_TO_NULL)
                                                        .build()));
      }
    }

    loc.setWaterBody(record.getCoreTerms().get(DwCATermIdentifier.waterBody.getIdentifier()));
    loc.setIslandGroup(record.getCoreTerms().get(DwCATermIdentifier.islandGroup.getIdentifier()));
    loc.setIsland(record.getCoreTerms().get(DwCATermIdentifier.island.getIdentifier()));

    /*
      Interpreting Country code
     */
    CharSequence rawCountry = record.getCoreTerms().get(DwCATermIdentifier.country.getIdentifier());
    CharSequence rawCountryCode = record.getCoreTerms().get(DwCATermIdentifier.countryCode.getIdentifier());
    if (rawCountry != null) {
      try {
        loc.setCountry(new CountryInterpreter().interpret(rawCountry.toString()));
      } catch (InterpretationException e) {
        fieldIssueMap.put(DwCATermIdentifier.country.name(), e.getIssues());
        fieldLineageMap.put(DwCATermIdentifier.country.name(), e.getLineages());
        e.getInterpretedValue().ifPresent((interpretedCountry) -> loc.setCountry(interpretedCountry.toString()));
      }
    }
    if (rawCountryCode != null) {
      try {
        loc.setCountryCode(new CountryCodeInterpreter().interpret(rawCountryCode.toString()));
      } catch (InterpretationException e) {
        fieldIssueMap.put(DwCATermIdentifier.country.name(), e.getIssues());
        fieldLineageMap.put(DwCATermIdentifier.country.name(), e.getLineages());
        e.getInterpretedValue()
          .ifPresent((interpretedCountryCode) -> loc.setCountryCode(interpretedCountryCode.toString()));
      }
    }

    loc.setStateProvince(record.getCoreTerms().get(DwCATermIdentifier.stateProvince.getIdentifier()));
    loc.setCounty(record.getCoreTerms().get(DwCATermIdentifier.county.getIdentifier()));
    loc.setMunicipality(record.getCoreTerms().get(DwCATermIdentifier.municipality.getIdentifier()));
    loc.setLocality(record.getCoreTerms().get(DwCATermIdentifier.locality.getIdentifier()));
    loc.setVerbatimLocality(record.getCoreTerms().get(DwCATermIdentifier.verbatimLocality.getIdentifier()));
    loc.setMinimumElevationInMeters(record.getCoreTerms()
                                      .get(DwCATermIdentifier.minimumElevationInMeters.getIdentifier()));
    loc.setMaximumElevationInMeters(record.getCoreTerms()
                                      .get(DwCATermIdentifier.maximumElevationInMeters.getIdentifier()));
    loc.setVerbatimElevation(record.getCoreTerms().get(DwCATermIdentifier.verbatimElevation.getIdentifier()));
    loc.setMaximumDepthInMeters(record.getCoreTerms().get(DwCATermIdentifier.maximumDepthInMeters.getIdentifier()));
    loc.setMinimumDepthInMeters(record.getCoreTerms().get(DwCATermIdentifier.minimumDepthInMeters.getIdentifier()));
    loc.setLocationAccordingTo(record.getCoreTerms().get(DwCATermIdentifier.locationAccordingTo.getIdentifier()));
    loc.setLocationRemarks(record.getCoreTerms().get(DwCATermIdentifier.locationRemarks.getIdentifier()));
    loc.setDecimalLatitude(record.getCoreTerms().get(DwCATermIdentifier.decimalLatitude.getIdentifier()));
    loc.setDecimalLongitude(record.getCoreTerms().get(DwCATermIdentifier.decimalLongitude.getIdentifier()));
    loc.setGeodeticDatum(record.getCoreTerms().get(DwCATermIdentifier.geodeticDatum.getIdentifier()));
    loc.setCoordinateUncertaintyInMeters(record.getCoreTerms()
                                           .get(DwCATermIdentifier.coordinateUncertaintyInMeters.getIdentifier()));
    loc.setCoordinatePrecision(record.getCoreTerms().get(DwCATermIdentifier.coordinatePrecision.getIdentifier()));
    loc.setPointRadiusSpatialFit(record.getCoreTerms().get(DwCATermIdentifier.pointRadiusSpatialFit.getIdentifier()));
    loc.setVerbatimCoordinates(record.getCoreTerms().get(DwCATermIdentifier.verbatimCoordinates.getIdentifier()));
    loc.setVerbatimLatitude(record.getCoreTerms().get(DwCATermIdentifier.verbatimLatitude.getIdentifier()));
    loc.setVerbatimLongitude(record.getCoreTerms().get(DwCATermIdentifier.verbatimLongitude.getIdentifier()));
    loc.setVerbatimCoordinateSystem(record.getCoreTerms()
                                      .get(DwCATermIdentifier.verbatimCoordinateSystem.getIdentifier()));
    loc.setVerbatimSRS(record.getCoreTerms().get(DwCATermIdentifier.verbatimSRS.getIdentifier()));
    loc.setFootprintWKT(record.getCoreTerms().get(DwCATermIdentifier.footprintWKT.getIdentifier()));
    loc.setFootprintSRS(record.getCoreTerms().get(DwCATermIdentifier.footprintSRS.getIdentifier()));
    loc.setFootprintSpatialFit(record.getCoreTerms().get(DwCATermIdentifier.footprintSpatialFit.getIdentifier()));
    loc.setGeoreferencedBy(record.getCoreTerms().get(DwCATermIdentifier.georeferencedBy.getIdentifier()));
    loc.setGeoreferencedDate(record.getCoreTerms().get(DwCATermIdentifier.georeferencedDate.getIdentifier()));
    loc.setGeoreferenceProtocol(record.getCoreTerms().get(DwCATermIdentifier.georeferenceProtocol.getIdentifier()));
    loc.setGeoreferenceSources(record.getCoreTerms().get(DwCATermIdentifier.georeferenceSources.getIdentifier()));
    loc.setGeoreferenceVerificationStatus(record.getCoreTerms()
                                            .get(DwCATermIdentifier.georeferenceVerificationStatus.getIdentifier()));
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
    //all issues and lineages are dumped on this object
    final IssueLineageRecord finalRecord = IssueLineageRecord.newBuilder()
      .setOccurenceId(record.getId())
      .setFieldLineageMap(fieldLineageMap)
      .setFieldIssuesMap(fieldIssueMap)
      .build();

    ctx.output(LOCATION_DATA_TAG, KV.of(loc.getOccurrenceID().toString(), loc));
    ctx.output(LOCATION_ISSUE_TAG, KV.of(loc.getOccurrenceID().toString(), finalRecord));
  }
}
