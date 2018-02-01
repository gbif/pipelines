package org.gbif.pipelines.core.functions.transforms;

import org.gbif.api.vocabulary.Continent;
import org.gbif.common.parsers.ContinentParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
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
    loc.setLocationID(record.getCoreTerms().get(DwcTerm.locationID.qualifiedName()));
    loc.setHigherGeographyID(record.getCoreTerms().get(DwcTerm.higherGeographyID.qualifiedName()));
    loc.setHigherGeography(record.getCoreTerms().get(DwcTerm.higherGeography.qualifiedName()));

    /*
      Interpreting Continent
     */

    CharSequence rawContinent = record.getCoreTerms().get(DwcTerm.continent.qualifiedName());

    if (rawContinent != null) {
      final ParseResult<Continent> parse = ContinentParser.getInstance().parse(rawContinent.toString());
      if (parse.isSuccessful()) {
        loc.setContinent(parse.getPayload().getTitle());
      } else {
        loc.setContinent(null);
        fieldIssueMap.put(DwcTerm.continent.name(),
                          Collections.singletonList(Issue.newBuilder()
                                                      .setRemark("Could not parse continent because " + (parse.getError()!=null?
                                                        parse.getError().getMessage():" is null"))
                                                      .setIssueType(IssueType.PARSE_ERROR)
                                                      .build()));
        fieldLineageMap.put(DwcTerm.continent.name(),
                            Collections.singletonList(Lineage.newBuilder()
                                                        .setRemark(
                                                          "Could not parse the continent or invalid value setting it to null")
                                                        .setLineageType(LineageType.SET_TO_NULL)
                                                        .build()));
      }
    }

    loc.setWaterBody(record.getCoreTerms().get(DwcTerm.waterBody.qualifiedName()));
    loc.setIslandGroup(record.getCoreTerms().get(DwcTerm.islandGroup.qualifiedName()));
    loc.setIsland(record.getCoreTerms().get(DwcTerm.island.qualifiedName()));

    /*
      Interpreting Country code
     */
    CharSequence rawCountry = record.getCoreTerms().get(DwcTerm.country.qualifiedName());
    CharSequence rawCountryCode = record.getCoreTerms().get(DwcTerm.countryCode.qualifiedName());
    if (rawCountry != null) {
      try {
        loc.setCountry(new CountryInterpreter().interpret(rawCountry.toString()));
      } catch (InterpretationException e) {
        fieldIssueMap.put(DwcTerm.country.name(), e.getIssues());
        fieldLineageMap.put(DwcTerm.country.name(), e.getLineages());
        e.getInterpretedValue().ifPresent((interpretedCountry) -> loc.setCountry(interpretedCountry.toString()));
      }
    }
    if (rawCountryCode != null) {
      try {
        loc.setCountryCode(new CountryCodeInterpreter().interpret(rawCountryCode.toString()));
      } catch (InterpretationException e) {
        fieldIssueMap.put(DwcTerm.country.name(), e.getIssues());
        fieldLineageMap.put(DwcTerm.country.name(), e.getLineages());
        e.getInterpretedValue()
          .ifPresent((interpretedCountryCode) -> loc.setCountryCode(interpretedCountryCode.toString()));
      }
    }

    loc.setStateProvince(record.getCoreTerms().get(DwcTerm.stateProvince.qualifiedName()));
    loc.setCounty(record.getCoreTerms().get(DwcTerm.county.qualifiedName()));
    loc.setMunicipality(record.getCoreTerms().get(DwcTerm.municipality.qualifiedName()));
    loc.setLocality(record.getCoreTerms().get(DwcTerm.locality.qualifiedName()));
    loc.setVerbatimLocality(record.getCoreTerms().get(DwcTerm.verbatimLocality.qualifiedName()));
    loc.setMinimumElevationInMeters(record.getCoreTerms()
                                      .get(DwcTerm.minimumElevationInMeters.qualifiedName()));
    loc.setMaximumElevationInMeters(record.getCoreTerms()
                                      .get(DwcTerm.maximumElevationInMeters.qualifiedName()));
    loc.setVerbatimElevation(record.getCoreTerms().get(DwcTerm.verbatimElevation.qualifiedName()));
    loc.setMaximumDepthInMeters(record.getCoreTerms().get(DwcTerm.maximumDepthInMeters.qualifiedName()));
    loc.setMinimumDepthInMeters(record.getCoreTerms().get(DwcTerm.minimumDepthInMeters.qualifiedName()));
    loc.setLocationAccordingTo(record.getCoreTerms().get(DwcTerm.locationAccordingTo.qualifiedName()));
    loc.setLocationRemarks(record.getCoreTerms().get(DwcTerm.locationRemarks.qualifiedName()));
    loc.setDecimalLatitude(record.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()));
    loc.setDecimalLongitude(record.getCoreTerms().get(DwcTerm.decimalLongitude.qualifiedName()));
    loc.setGeodeticDatum(record.getCoreTerms().get(DwcTerm.geodeticDatum.qualifiedName()));
    loc.setCoordinateUncertaintyInMeters(record.getCoreTerms()
                                           .get(DwcTerm.coordinateUncertaintyInMeters.qualifiedName()));
    loc.setCoordinatePrecision(record.getCoreTerms().get(DwcTerm.coordinatePrecision.qualifiedName()));
    loc.setPointRadiusSpatialFit(record.getCoreTerms().get(DwcTerm.pointRadiusSpatialFit.qualifiedName()));
    loc.setVerbatimCoordinates(record.getCoreTerms().get(DwcTerm.verbatimCoordinates.qualifiedName()));
    loc.setVerbatimLatitude(record.getCoreTerms().get(DwcTerm.verbatimLatitude.qualifiedName()));
    loc.setVerbatimLongitude(record.getCoreTerms().get(DwcTerm.verbatimLongitude.qualifiedName()));
    loc.setVerbatimCoordinateSystem(record.getCoreTerms()
                                      .get(DwcTerm.verbatimCoordinateSystem.qualifiedName()));
    loc.setVerbatimSRS(record.getCoreTerms().get(DwcTerm.verbatimSRS.qualifiedName()));
    loc.setFootprintWKT(record.getCoreTerms().get(DwcTerm.footprintWKT.qualifiedName()));
    loc.setFootprintSRS(record.getCoreTerms().get(DwcTerm.footprintSRS.qualifiedName()));
    loc.setFootprintSpatialFit(record.getCoreTerms().get(DwcTerm.footprintSpatialFit.qualifiedName()));
    loc.setGeoreferencedBy(record.getCoreTerms().get(DwcTerm.georeferencedBy.qualifiedName()));
    loc.setGeoreferencedDate(record.getCoreTerms().get(DwcTerm.georeferencedDate.qualifiedName()));
    loc.setGeoreferenceProtocol(record.getCoreTerms().get(DwcTerm.georeferenceProtocol.qualifiedName()));
    loc.setGeoreferenceSources(record.getCoreTerms().get(DwcTerm.georeferenceSources.qualifiedName()));
    loc.setGeoreferenceVerificationStatus(record.getCoreTerms()
                                            .get(DwcTerm.georeferenceVerificationStatus.qualifiedName()));
    loc.setGeoreferenceRemarks(record.getCoreTerms().get(DwcTerm.georeferenceRemarks.qualifiedName()));
    loc.setInstitutionID(record.getCoreTerms().get(DwcTerm.institutionID.qualifiedName()));
    loc.setCollectionID(record.getCoreTerms().get(DwcTerm.collectionID.qualifiedName()));
    loc.setDatasetID(record.getCoreTerms().get(DwcTerm.datasetID.qualifiedName()));
    loc.setInstitutionCode(record.getCoreTerms().get(DwcTerm.institutionCode.qualifiedName()));
    loc.setCollectionCode(record.getCoreTerms().get(DwcTerm.collectionCode.qualifiedName()));
    loc.setDatasetName(record.getCoreTerms().get(DwcTerm.datasetName.qualifiedName()));
    loc.setOwnerInstitutionCode(record.getCoreTerms().get(DwcTerm.ownerInstitutionCode.qualifiedName()));
    loc.setDynamicProperties(record.getCoreTerms().get(DwcTerm.dynamicProperties.qualifiedName()));
    loc.setInformationWithheld(record.getCoreTerms().get(DwcTerm.informationWithheld.qualifiedName()));
    loc.setDataGeneralizations(record.getCoreTerms().get(DwcTerm.dataGeneralizations.qualifiedName()));
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
