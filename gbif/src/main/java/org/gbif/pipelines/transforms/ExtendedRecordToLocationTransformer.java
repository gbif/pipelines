package org.gbif.pipelines.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;

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
    private final TupleTag<KV<String, Location>> locationDataTag = new TupleTag<KV<String, Location>>() {
    };
    private final TupleTag<KV<String, IssueLineageRecord>> locationIssueTag = new TupleTag<KV<String, IssueLineageRecord>>() {
    };

    public TupleTag<KV<String, Location>> getLocationDataTag() {
        return locationDataTag;
    }

    public TupleTag<KV<String, IssueLineageRecord>> getLocationIssueTag() {
        return locationIssueTag;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        ExtendedRecord record = ctx.element();
        Location loc = new Location();

        //mapping raw record with interpreted ones
        loc.setOccurrenceID(record.getId());
        loc.setLocationID(record.getCoreTerms().get(DwcTerm.locationID.qualifiedName()));
        loc.setHigherGeographyID(record.getCoreTerms().get(DwcTerm.higherGeographyID.qualifiedName()));
        loc.setHigherGeography(record.getCoreTerms().get(DwcTerm.higherGeography.qualifiedName()));
        loc.setWaterBody(record.getCoreTerms().get(DwcTerm.waterBody.qualifiedName()));
        loc.setIslandGroup(record.getCoreTerms().get(DwcTerm.islandGroup.qualifiedName()));
        loc.setIsland(record.getCoreTerms().get(DwcTerm.island.qualifiedName()));

    /*
      Interpreting Country code
     */

        final IssueLineageRecord issueLineageRecord = Interpretation
                .of(record)
                .using(LocationInterpreter.interpretCountry(loc))
                .using(LocationInterpreter.interpretCountryCode(loc))
                .using(LocationInterpreter.interpretContinent(loc)).getIssueLineageRecord(record.getId());

        loc.setStateProvince(record.getCoreTerms().get(DwcTerm.stateProvince.qualifiedName()));
        loc.setCounty(record.getCoreTerms().get(DwcTerm.county.qualifiedName()));
        loc.setMunicipality(record.getCoreTerms().get(DwcTerm.municipality.qualifiedName()));
        loc.setLocality(record.getCoreTerms().get(DwcTerm.locality.qualifiedName()));
        loc.setVerbatimLocality(record.getCoreTerms().get(DwcTerm.verbatimLocality.qualifiedName()));
        loc.setMinimumElevationInMeters(record.getCoreTerms().get(DwcTerm.minimumElevationInMeters.qualifiedName()));
        loc.setMaximumElevationInMeters(record.getCoreTerms().get(DwcTerm.maximumElevationInMeters.qualifiedName()));
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
        loc.setVerbatimCoordinateSystem(record.getCoreTerms().get(DwcTerm.verbatimCoordinateSystem.qualifiedName()));
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
        issueLineageRecord.setOccurenceId(record.getId());
        ctx.output(locationDataTag, KV.of(loc.getOccurrenceID().toString(), loc));
        ctx.output(locationIssueTag, KV.of(loc.getOccurrenceID().toString(), issueLineageRecord));
    }
}
