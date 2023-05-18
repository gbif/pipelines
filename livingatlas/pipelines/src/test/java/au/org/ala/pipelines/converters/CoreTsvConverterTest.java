package au.org.ala.pipelines.converters;

import au.org.ala.pipelines.transforms.IndexRecordTransform;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ALAAttributionRecord;
import org.gbif.pipelines.io.avro.ALASensitivityRecord;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;
import org.gbif.pipelines.io.avro.AgentIdentifier;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.Diagnostic;
import org.gbif.pipelines.io.avro.EntityReference;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Image;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MatchType;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.Nomenclature;
import org.gbif.pipelines.io.avro.ParsedName;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.Status;
import org.gbif.pipelines.io.avro.TaxonProfile;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.VocabularyConcept;
import org.junit.Assert;
import org.junit.Test;

public class CoreTsvConverterTest {

  @Test
  public void converterTest() {
    // Expected
    // tr = 1, br = 2, lr = 3, trx = 4, atxr = 5, aur = 6, ir = 7, asr = 8
    String[] expected = {
      // DWC Terms
      "\"aur_uuid\"", // DwcTerm.occurrenceID
      "\"raw_er_catalogNumber\"", // DwcTerm.catalogNumber
      "\"raw_er_collectionCode\"", // DwcTerm.collectionCode
      "\"raw_er_institutionCode\"", // DwcTerm.institutionCode
      "\"raw_er_recordNumber\"", // DwcTerm.recordNumber
      "\"br_basisOfRecord\"", // DwcTerm.basisOfRecord
      "\"br_recordedBy_1|br_recordedBy_2\"", // DwcTerm.recordedBy
      "\"br_occurrenceStatus\"", // DwcTerm.occurrenceStatus
      "\"222\"", // DwcTerm.individualCount
      "\"atxr_ScientificName\"", // DwcTerm.scientificName
      "\"atxr_TaxonConceptID\"", // DwcTerm.taxonConceptID
      "\"atxr_taxonrank\"", // DwcTerm.taxonRank
      "\"atxr_Kingdom\"", // DwcTerm.kingdom
      "\"atxr_Phylum\"", // DwcTerm.phylum
      "\"atxr_Classs\"", // DwcTerm.class_
      "\"atxr_Order\"", // DwcTerm.order
      "\"atxr_Family\"", // DwcTerm.family
      "\"atxr_Genus\"", // DwcTerm.genus
      "\"atxr_VernacularName\"", // DwcTerm.vernacularName
      "\"3.33333333333E11\"", // DwcTerm.decimalLatitude
      "\"3.333333333333E12\"", // DwcTerm.decimalLongitude
      "\"EPSG:4326\"", // DwcTerm.geodeticDatum
      "\"3.3333333333333E13\"", // DwcTerm.coordinateUncertaintyInMeters
      "\"333.0\"", // DwcTerm.maximumElevationInMeters
      "\"33.0\"", // DwcTerm.minimumElevationInMeters
      "\"33333.0\"", // DwcTerm.minimumDepthInMeters
      "\"333333.0\"", // DwcTerm.maximumDepthInMeters
      "\"lr_country\"", // DwcTerm.country
      "\"lr_stateProvince\"", // DwcTerm.stateProvince
      "\"lr_locality\"", // DwcTerm.locality
      "\"raw_er_locationRemarks\"", // DwcTerm.locationRemarks
      "\"111\"", // DwcTerm.year
      "\"1111\"", // DwcTerm.month
      "\"11111\"", // DwcTerm.day
      "\"raw_er_eventDate\"", // DwcTerm.eventDate
      "\"raw_er_eventID\"", // DwcTerm.eventID
      "\"br_identifiedBy\"", // DwcTerm.identifiedBy
      "\"raw_er_occurrenceRemarks\"", // DwcTerm.occurrenceRemarks
      "\"\"", // DwcTerm.dataGeneralizations
      "\"br_otherCatalogNumbers\"", // DwcTerm.otherCatalogNumbers
      "\"raw_er_acceptedNameUsage\"", // DwcTerm.acceptedNameUsage
      "\"raw_er_acceptedNameUsageID\"", // DwcTerm.acceptedNameUsageID
      "\"\"", // DwcTerm.associatedOccurrences
      "\"raw_er_associatedReferences\"", // DwcTerm.associatedReferences
      "\"raw_er_associatedSequences\"", // DwcTerm.associatedSequences
      "\"raw_er_associatedTaxa\"", // DwcTerm.associatedTaxa
      "\"raw_er_behavior\"", // DwcTerm.behavior
      "\"raw_er_collectionID\"", // DwcTerm.collectionID
      "\"lr_continent\"", // DwcTerm.continent
      "\"3.33333333333333E14\"", // DwcTerm.coordinatePrecision
      "\"lr_countryCode\"", // DwcTerm.countryCode
      "\"raw_er_county\"", // DwcTerm.county
      "\"br_datasetID\"", // DwcTerm.datasetID
      "\"br_datasetName\"", // DwcTerm.datasetName
      "\"2002\"", // DwcTerm.dateIdentified
      "\"raw_er_disposition\"", // DwcTerm.disposition
      "\"raw_er_dynamicProperties\"", // DwcTerm.dynamicProperties
      "\"1111111\"", // DwcTerm.endDayOfYear
      "\"br_establishmentMeans\"", // DwcTerm.establishmentMeans
      "\"raw_er_eventRemarks\"", // DwcTerm.eventRemarks
      "\"raw_er_eventTime\"", // DwcTerm.eventTime
      "\"raw_er_fieldNotes\"", // DwcTerm.fieldNotes
      "\"raw_er_fieldNumber\"", // DwcTerm.fieldNumber
      "\"raw_er_footprintSpatialFit\"", // DwcTerm.footprintSpatialFit
      "\"raw_er_footprintSRS\"", // DwcTerm.footprintSRS
      "\"lr_footprintWKT\"", // DwcTerm.footprintWKT
      "\"raw_er_georeferencedBy\"", // DwcTerm.georeferencedBy
      "\"lr_georeferencedDate\"", // DwcTerm.georeferencedDate
      "\"raw_er_georeferenceProtocol\"", // DwcTerm.georeferenceProtocol
      "\"raw_er_georeferenceRemarks\"", // DwcTerm.georeferenceRemarks
      "\"raw_er_georeferenceSources\"", // DwcTerm.georeferenceSources
      "\"raw_er_georeferenceVerificationStatus\"", // DwcTerm.georeferenceVerificationStatus
      "\"raw_er_habitat\"", // DwcTerm.habitat
      "\"raw_er_higherClassification\"", // DwcTerm.higherClassification
      "\"raw_er_higherGeography\"", // DwcTerm.higherGeography
      "\"raw_er_higherGeographyID\"", // DwcTerm.higherGeographyID
      "\"raw_er_identificationID\"", // DwcTerm.identificationID
      "\"raw_er_identificationQualifier\"", // DwcTerm.identificationQualifier
      "\"raw_er_identificationReferences\"", // DwcTerm.identificationReferences
      "\"raw_er_identificationRemarks\"", // DwcTerm.identificationRemarks
      "\"raw_er_identificationVerificationStatus\"", // DwcTerm.identificationVerificationStatus
      "\"\"", // DwcTerm.informationWithheld
      "\"raw_er_infraspecificEpithet\"", // DwcTerm.infraspecificEpithet
      "\"raw_er_institutionID\"", // DwcTerm.institutionID
      "\"raw_er_island\"", // DwcTerm.island
      "\"raw_er_islandGroup\"", // DwcTerm.islandGroup
      "\"{concept: br_lifeStage, lineage: [br_lifeStageLineage]}\"", // DwcTerm.lifeStage
      "\"raw_er_locationAccordingTo\"", // DwcTerm.locationAccordingTo
      "\"raw_er_locationID\"", // DwcTerm.locationID
      "\"3.3333333333E10\"", // DwcTerm.maximumDistanceAboveSurfaceInMeters
      "\"raw_er_measurementAccuracy\"", // DwcTerm.measurementAccuracy
      "\"raw_er_measurementDeterminedBy\"", // DwcTerm.measurementDeterminedBy
      "\"raw_er_measurementDeterminedDate\"", // DwcTerm.measurementDeterminedDate
      "\"raw_er_measurementID\"", // DwcTerm.measurementID
      "\"raw_er_measurementMethod\"", // DwcTerm.measurementMethod
      "\"raw_er_measurementRemarks\"", // DwcTerm.measurementRemarks
      "\"raw_er_measurementType\"", // DwcTerm.measurementType
      "\"raw_er_measurementUnit\"", // DwcTerm.measurementUnit
      "\"raw_er_measurementValue\"", // DwcTerm.measurementValue
      "\"raw_er_municipality\"", // DwcTerm.municipality
      "\"raw_er_nameAccordingTo\"", // DwcTerm.nameAccordingTo
      "\"raw_er_nameAccordingToID\"", // DwcTerm.nameAccordingToID
      "\"raw_er_namePublishedIn\"", // DwcTerm.namePublishedIn
      "\"raw_er_namePublishedInID\"", // DwcTerm.namePublishedInID
      "\"raw_er_namePublishedInYear\"", // DwcTerm.namePublishedInYear
      "\"raw_er_nomenclaturalCode\"", // DwcTerm.nomenclaturalCode
      "\"raw_er_nomenclaturalStatus\"", // DwcTerm.nomenclaturalStatus
      "\"raw_er_organismID\"", // DwcTerm.organismID
      "\"2222.0\"", // DwcTerm.organismQuantity
      "\"br_organismQuantityType\"", // DwcTerm.organismQuantityType
      "\"raw_er_originalNameUsage\"", // DwcTerm.originalNameUsage
      "\"raw_er_originalNameUsageID\"", // DwcTerm.originalNameUsageID
      "\"raw_er_ownerInstitutionCode\"", // DwcTerm.ownerInstitutionCode
      "\"raw_er_parentNameUsage\"", // DwcTerm.parentNameUsage
      "\"raw_er_parentNameUsageID\"", // DwcTerm.parentNameUsageID
      "\"raw_er_pointRadiusSpatialFit\"", // DwcTerm.pointRadiusSpatialFit
      "\"br_preparations\"", // DwcTerm.preparations
      "\"raw_er_previousIdentifications\"", // DwcTerm.previousIdentifications
      "\"raw_er_relatedResourceID\"", // DwcTerm.relatedResourceID
      "\"raw_er_relationshipAccordingTo\"", // DwcTerm.relationshipAccordingTo
      "\"3.333333333E9\"", // DwcTerm.minimumDistanceAboveSurfaceInMeters
      "\"raw_er_relationshipEstablishedDate\"", // DwcTerm.relationshipEstablishedDate
      "\"raw_er_relationshipOfResource\"", // DwcTerm.relationshipOfResource
      "\"raw_er_relationshipRemarks\"", // DwcTerm.relationshipRemarks
      "\"raw_er_reproductiveCondition\"", // DwcTerm.reproductiveCondition
      "\"raw_er_resourceID\"", // DwcTerm.resourceID
      "\"raw_er_resourceRelationshipID\"", // DwcTerm.resourceRelationshipID
      "\"raw_er_samplingEffort\"", // DwcTerm.samplingEffort
      "\"br_samplingProtocol\"", // DwcTerm.samplingProtocol
      "\"atxr_ScientificNameAuthorship\"", // DwcTerm.scientificNameAuthorship
      "\"raw_er_scientificNameID\"", // DwcTerm.scientificNameID
      "\"br_sex\"", // DwcTerm.sex
      "\"raw_er_specificEpithet\"", // DwcTerm.specificEpithet
      "\"111111\"", // DwcTerm.startDayOfYear
      "\"raw_er_subgenus\"", // DwcTerm.subgenus
      "\"raw_er_taxonID\"", // DwcTerm.taxonID
      "\"raw_er_taxonomicStatus\"", // DwcTerm.taxonomicStatus
      "\"raw_er_taxonRemarks\"", // DwcTerm.taxonRemarks
      "\"br_typeStatus\"", // DwcTerm.typeStatus
      "\"raw_er_verbatimCoordinates\"", // DwcTerm.verbatimCoordinates
      "\"raw_er_verbatimCoordinateSystem\"", // DwcTerm.verbatimCoordinateSystem
      "\"raw_er_verbatimDepth\"", // DwcTerm.verbatimDepth
      "\"raw_er_verbatimElevation\"", // DwcTerm.verbatimElevation
      "\"raw_er_verbatimEventDate\"", // DwcTerm.verbatimEventDate
      "\"raw_er_verbatimLatitude\"", // DwcTerm.verbatimLatitude
      "\"raw_er_verbatimLocality\"", // DwcTerm.verbatimLocality
      "\"raw_er_verbatimLongitude\"", // DwcTerm.verbatimLongitude
      "\"raw_er_verbatimSRS\"", // DwcTerm.verbatimSRS
      "\"raw_er_verbatimTaxonRank\"", // DwcTerm.verbatimTaxonRank
      "\"lr_waterBody\"", // DwcTerm.waterBody
      "\"raw_er_occurrenceAttributes\"", // http://rs.tdwg.org/dwc/terms/occurrenceAttributes
      // DC Terms
      "\"br_References\"", // DcTerm.references
      "\"raw_er_accessRights\"", // DcTerm.accessRights
      "\"raw_er_bibliographicCitation\"", // DcTerm.bibliographicCitation
      "\"raw_er_language\"", // DcTerm.language
      "\"br_license\"", // DcTerm.license
      "\"2001\"", // DcTerm.modified
      "\"raw_er_rights\"", // DcTerm.rights
      "\"raw_er_rightsHolder\"", // DcTerm.rightsHolder
      "\"raw_er_source\"", // DcTerm.source
      "\"raw_er_type\"", // DcTerm.type
      // ALA Terms
      "\"raw_er_photographer\"", // http://rs.ala.org.au/terms/1.0/photographer
      "\"raw_er_northing\"", // http://rs.ala.org.au/terms/1.0/northing
      "\"raw_er_easting\"", // http://rs.ala.org.au/terms/1.0/easting
      "\"atxr_Species\"", // http://rs.ala.org.au/terms/1.0/species
      "\"raw_er_subfamily\"", // http://rs.ala.org.au/terms/1.0/subfamily
      "\"raw_er_subspecies\"", // http://rs.ala.org.au/terms/1.0/subspecies
      "\"raw_er_superfamily\"", // http://rs.ala.org.au/terms/1.0/superfamily
      "\"raw_er_zone\"", // http://rs.ala.org.au/terms/1.0/zone
      // ABCD Terms
      "\"raw_er_abcdIdentificationQualifier\"", // http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifier
      "\"raw_er_abcdIdentificationQualifierInsertionPoint\"", // http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifierInsertionPoint
      "\"raw_er_abcdTypeStatus\"", // http://rs.tdwg.org/abcd/terms/abcdTypeStatus
      "\"br_typifiedName\"", // http://rs.tdwg.org/abcd/terms/typifiedName
      // HISPID Terms
      "\"raw_er_secondaryCollectors\"", // http://hiscom.chah.org.au/hispid/terms/secondaryCollectors
      "\"raw_er_identifierRole\"", // http://hiscom.chah.org.au/hispid/terms/identifierRole
      // GGBN Terms
      "\"raw_er_loanDate\"", // http://data.ggbn.org/schemas/ggbn/terms/loanDate
      "\"raw_er_loanDestination\"", // http://data.ggbn.org/schemas/ggbn/terms/loanDestination
      "\"raw_er_loanIdentifier\"", // http://data.ggbn.org/schemas/ggbn/terms/loanIdentifier
      "\"raw_er_locationAttributes\"", // http://rs.tdwg.org/dwc/terms/locationAttributes
      "\"raw_er_eventAttributes\"", // http://rs.tdwg.org/dwc/terms/eventAttributes
      // Other Terms
      "\"5\"", // taxonRankID
      // GBIF Terms
      "\"raw_er_recordedByID\"" // GbifTerm.recordedByID
    };

    // State
    Map<String, String> core = new HashMap<>();
    // DWC Terms
    core.put(DwcTerm.occurrenceID.qualifiedName(), "raw_er_" + DwcTerm.occurrenceID.simpleName());
    core.put(DwcTerm.catalogNumber.qualifiedName(), "raw_er_" + DwcTerm.catalogNumber.simpleName());
    core.put(
        DwcTerm.collectionCode.qualifiedName(), "raw_er_" + DwcTerm.collectionCode.simpleName());
    core.put(
        DwcTerm.institutionCode.qualifiedName(), "raw_er_" + DwcTerm.institutionCode.simpleName());
    core.put(DwcTerm.recordNumber.qualifiedName(), "raw_er_" + DwcTerm.recordNumber.simpleName());
    core.put(DwcTerm.basisOfRecord.qualifiedName(), "raw_er_" + DwcTerm.basisOfRecord.simpleName());
    core.put(DwcTerm.recordedBy.qualifiedName(), "raw_er_" + DwcTerm.recordedBy.simpleName());
    core.put(
        DwcTerm.occurrenceStatus.qualifiedName(),
        "raw_er_" + DwcTerm.occurrenceStatus.simpleName());
    core.put(
        DwcTerm.individualCount.qualifiedName(), "raw_er_" + DwcTerm.individualCount.simpleName());
    core.put(
        DwcTerm.scientificName.qualifiedName(), "raw_er_" + DwcTerm.scientificName.simpleName());
    core.put(
        DwcTerm.taxonConceptID.qualifiedName(), "raw_er_" + DwcTerm.taxonConceptID.simpleName());
    core.put(DwcTerm.taxonRank.qualifiedName(), "raw_er_" + DwcTerm.taxonRank.simpleName());
    core.put(DwcTerm.kingdom.qualifiedName(), "raw_er_" + DwcTerm.kingdom.simpleName());
    core.put(DwcTerm.phylum.qualifiedName(), "raw_er_" + DwcTerm.phylum.simpleName());
    core.put(DwcTerm.class_.qualifiedName(), "raw_er_" + DwcTerm.class_.simpleName());
    core.put(DwcTerm.order.qualifiedName(), "raw_er_" + DwcTerm.order.simpleName());
    core.put(DwcTerm.family.qualifiedName(), "raw_er_" + DwcTerm.family.simpleName());
    core.put(DwcTerm.genus.qualifiedName(), "raw_er_" + DwcTerm.genus.simpleName());
    core.put(
        DwcTerm.vernacularName.qualifiedName(), "raw_er_" + DwcTerm.vernacularName.simpleName());
    core.put(
        DwcTerm.decimalLatitude.qualifiedName(), "raw_er_" + DwcTerm.decimalLatitude.simpleName());
    core.put(
        DwcTerm.decimalLongitude.qualifiedName(),
        "raw_er_" + DwcTerm.decimalLongitude.simpleName());
    core.put(DwcTerm.geodeticDatum.qualifiedName(), "raw_er_" + DwcTerm.geodeticDatum.simpleName());
    core.put(
        DwcTerm.coordinateUncertaintyInMeters.qualifiedName(),
        "raw_er_" + DwcTerm.coordinateUncertaintyInMeters.simpleName());
    core.put(
        DwcTerm.maximumElevationInMeters.qualifiedName(),
        "raw_er_" + DwcTerm.maximumElevationInMeters.simpleName());
    core.put(
        DwcTerm.minimumElevationInMeters.qualifiedName(),
        "raw_er_" + DwcTerm.minimumElevationInMeters.simpleName());
    core.put(
        DwcTerm.minimumDepthInMeters.qualifiedName(),
        "raw_er_" + DwcTerm.minimumDepthInMeters.simpleName());
    core.put(
        DwcTerm.maximumDepthInMeters.qualifiedName(),
        "raw_er_" + DwcTerm.maximumDepthInMeters.simpleName());
    core.put(DwcTerm.country.qualifiedName(), "raw_er_" + DwcTerm.country.simpleName());
    core.put(DwcTerm.stateProvince.qualifiedName(), "raw_er_" + DwcTerm.stateProvince.simpleName());
    core.put(DwcTerm.locality.qualifiedName(), "raw_er_" + DwcTerm.locality.simpleName());
    core.put(
        DwcTerm.locationRemarks.qualifiedName(), "raw_er_" + DwcTerm.locationRemarks.simpleName());
    core.put(DwcTerm.year.qualifiedName(), "raw_er_" + DwcTerm.year.simpleName());
    core.put(DwcTerm.month.qualifiedName(), "raw_er_" + DwcTerm.month.simpleName());
    core.put(DwcTerm.day.qualifiedName(), "raw_er_" + DwcTerm.day.simpleName());
    core.put(DwcTerm.eventDate.qualifiedName(), "raw_er_" + DwcTerm.eventDate.simpleName());
    core.put(DwcTerm.eventID.qualifiedName(), "raw_er_" + DwcTerm.eventID.simpleName());
    core.put(DwcTerm.identifiedBy.qualifiedName(), "raw_er_" + DwcTerm.identifiedBy.simpleName());
    core.put(
        DwcTerm.occurrenceRemarks.qualifiedName(),
        "raw_er_" + DwcTerm.occurrenceRemarks.simpleName());
    core.put(
        DwcTerm.dataGeneralizations.qualifiedName(),
        "raw_er_" + DwcTerm.dataGeneralizations.simpleName());
    core.put(
        DwcTerm.otherCatalogNumbers.qualifiedName(),
        "raw_er_" + DwcTerm.otherCatalogNumbers.simpleName());
    core.put(
        DwcTerm.acceptedNameUsage.qualifiedName(),
        "raw_er_" + DwcTerm.acceptedNameUsage.simpleName());
    core.put(
        DwcTerm.acceptedNameUsageID.qualifiedName(),
        "raw_er_" + DwcTerm.acceptedNameUsageID.simpleName());
    core.put(
        DwcTerm.associatedOccurrences.qualifiedName(),
        "raw_er_" + DwcTerm.associatedOccurrences.simpleName());
    core.put(
        DwcTerm.associatedReferences.qualifiedName(),
        "raw_er_" + DwcTerm.associatedReferences.simpleName());
    core.put(
        DwcTerm.associatedSequences.qualifiedName(),
        "raw_er_" + DwcTerm.associatedSequences.simpleName());
    core.put(
        DwcTerm.associatedTaxa.qualifiedName(), "raw_er_" + DwcTerm.associatedTaxa.simpleName());
    core.put(DwcTerm.behavior.qualifiedName(), "raw_er_" + DwcTerm.behavior.simpleName());
    core.put(DwcTerm.collectionID.qualifiedName(), "raw_er_" + DwcTerm.collectionID.simpleName());
    core.put(DwcTerm.continent.qualifiedName(), "raw_er_" + DwcTerm.continent.simpleName());
    core.put(
        DwcTerm.coordinatePrecision.qualifiedName(),
        "raw_er_" + DwcTerm.coordinatePrecision.simpleName());
    core.put(DwcTerm.countryCode.qualifiedName(), "raw_er_" + DwcTerm.countryCode.simpleName());
    core.put(DwcTerm.county.qualifiedName(), "raw_er_" + DwcTerm.county.simpleName());
    core.put(DwcTerm.datasetID.qualifiedName(), "raw_er_" + DwcTerm.datasetID.simpleName());
    core.put(DwcTerm.datasetName.qualifiedName(), "raw_er_" + DwcTerm.datasetName.simpleName());
    core.put(
        DwcTerm.dateIdentified.qualifiedName(), "raw_er_" + DwcTerm.dateIdentified.simpleName());
    core.put(DwcTerm.disposition.qualifiedName(), "raw_er_" + DwcTerm.disposition.simpleName());
    core.put(
        DwcTerm.dynamicProperties.qualifiedName(),
        "raw_er_" + DwcTerm.dynamicProperties.simpleName());
    core.put(DwcTerm.endDayOfYear.qualifiedName(), "raw_er_" + DwcTerm.endDayOfYear.simpleName());
    core.put(
        DwcTerm.establishmentMeans.qualifiedName(),
        "raw_er_" + DwcTerm.establishmentMeans.simpleName());
    core.put(DwcTerm.eventRemarks.qualifiedName(), "raw_er_" + DwcTerm.eventRemarks.simpleName());
    core.put(DwcTerm.eventTime.qualifiedName(), "raw_er_" + DwcTerm.eventTime.simpleName());
    core.put(DwcTerm.fieldNotes.qualifiedName(), "raw_er_" + DwcTerm.fieldNotes.simpleName());
    core.put(DwcTerm.fieldNumber.qualifiedName(), "raw_er_" + DwcTerm.fieldNumber.simpleName());
    core.put(
        DwcTerm.footprintSpatialFit.qualifiedName(),
        "raw_er_" + DwcTerm.footprintSpatialFit.simpleName());
    core.put(DwcTerm.footprintSRS.qualifiedName(), "raw_er_" + DwcTerm.footprintSRS.simpleName());
    core.put(DwcTerm.footprintWKT.qualifiedName(), "raw_er_" + DwcTerm.footprintWKT.simpleName());
    core.put(
        DwcTerm.georeferencedBy.qualifiedName(), "raw_er_" + DwcTerm.georeferencedBy.simpleName());
    core.put(
        DwcTerm.georeferencedDate.qualifiedName(),
        "raw_er_" + DwcTerm.georeferencedDate.simpleName());
    core.put(
        DwcTerm.georeferenceProtocol.qualifiedName(),
        "raw_er_" + DwcTerm.georeferenceProtocol.simpleName());
    core.put(
        DwcTerm.georeferenceRemarks.qualifiedName(),
        "raw_er_" + DwcTerm.georeferenceRemarks.simpleName());
    core.put(
        DwcTerm.georeferenceSources.qualifiedName(),
        "raw_er_" + DwcTerm.georeferenceSources.simpleName());
    core.put(
        DwcTerm.georeferenceVerificationStatus.qualifiedName(),
        "raw_er_" + DwcTerm.georeferenceVerificationStatus.simpleName());
    core.put(DwcTerm.habitat.qualifiedName(), "raw_er_" + DwcTerm.habitat.simpleName());
    core.put(
        DwcTerm.higherClassification.qualifiedName(),
        "raw_er_" + DwcTerm.higherClassification.simpleName());
    core.put(
        DwcTerm.higherGeography.qualifiedName(), "raw_er_" + DwcTerm.higherGeography.simpleName());
    core.put(
        DwcTerm.higherGeographyID.qualifiedName(),
        "raw_er_" + DwcTerm.higherGeographyID.simpleName());
    core.put(
        DwcTerm.identificationID.qualifiedName(),
        "raw_er_" + DwcTerm.identificationID.simpleName());
    core.put(
        DwcTerm.identificationQualifier.qualifiedName(),
        "raw_er_" + DwcTerm.identificationQualifier.simpleName());
    core.put(
        DwcTerm.identificationReferences.qualifiedName(),
        "raw_er_" + DwcTerm.identificationReferences.simpleName());
    core.put(
        DwcTerm.identificationRemarks.qualifiedName(),
        "raw_er_" + DwcTerm.identificationRemarks.simpleName());
    core.put(
        DwcTerm.identificationVerificationStatus.qualifiedName(),
        "raw_er_" + DwcTerm.identificationVerificationStatus.simpleName());
    core.put(
        DwcTerm.informationWithheld.qualifiedName(),
        "raw_er_" + DwcTerm.informationWithheld.simpleName());
    core.put(
        DwcTerm.infraspecificEpithet.qualifiedName(),
        "raw_er_" + DwcTerm.infraspecificEpithet.simpleName());
    core.put(DwcTerm.institutionID.qualifiedName(), "raw_er_" + DwcTerm.institutionID.simpleName());
    core.put(DwcTerm.island.qualifiedName(), "raw_er_" + DwcTerm.island.simpleName());
    core.put(DwcTerm.islandGroup.qualifiedName(), "raw_er_" + DwcTerm.islandGroup.simpleName());
    core.put(DwcTerm.lifeStage.qualifiedName(), "raw_er_" + DwcTerm.lifeStage.simpleName());
    core.put(
        DwcTerm.locationAccordingTo.qualifiedName(),
        "raw_er_" + DwcTerm.locationAccordingTo.simpleName());
    core.put(DwcTerm.locationID.qualifiedName(), "raw_er_" + DwcTerm.locationID.simpleName());
    core.put(
        DwcTerm.maximumDistanceAboveSurfaceInMeters.qualifiedName(),
        "raw_er_" + DwcTerm.maximumDistanceAboveSurfaceInMeters.simpleName());
    core.put(
        DwcTerm.measurementAccuracy.qualifiedName(),
        "raw_er_" + DwcTerm.measurementAccuracy.simpleName());
    core.put(
        DwcTerm.measurementDeterminedBy.qualifiedName(),
        "raw_er_" + DwcTerm.measurementDeterminedBy.simpleName());
    core.put(
        DwcTerm.measurementDeterminedDate.qualifiedName(),
        "raw_er_" + DwcTerm.measurementDeterminedDate.simpleName());
    core.put(DwcTerm.measurementID.qualifiedName(), "raw_er_" + DwcTerm.measurementID.simpleName());
    core.put(
        DwcTerm.measurementMethod.qualifiedName(),
        "raw_er_" + DwcTerm.measurementMethod.simpleName());
    core.put(
        DwcTerm.measurementRemarks.qualifiedName(),
        "raw_er_" + DwcTerm.measurementRemarks.simpleName());
    core.put(
        DwcTerm.measurementType.qualifiedName(), "raw_er_" + DwcTerm.measurementType.simpleName());
    core.put(
        DwcTerm.measurementUnit.qualifiedName(), "raw_er_" + DwcTerm.measurementUnit.simpleName());
    core.put(
        DwcTerm.measurementValue.qualifiedName(),
        "raw_er_" + DwcTerm.measurementValue.simpleName());
    core.put(DwcTerm.municipality.qualifiedName(), "raw_er_" + DwcTerm.municipality.simpleName());
    core.put(
        DwcTerm.nameAccordingTo.qualifiedName(), "raw_er_" + DwcTerm.nameAccordingTo.simpleName());
    core.put(
        DwcTerm.nameAccordingToID.qualifiedName(),
        "raw_er_" + DwcTerm.nameAccordingToID.simpleName());
    core.put(
        DwcTerm.namePublishedIn.qualifiedName(), "raw_er_" + DwcTerm.namePublishedIn.simpleName());
    core.put(
        DwcTerm.namePublishedInID.qualifiedName(),
        "raw_er_" + DwcTerm.namePublishedInID.simpleName());
    core.put(
        DwcTerm.namePublishedInYear.qualifiedName(),
        "raw_er_" + DwcTerm.namePublishedInYear.simpleName());
    core.put(
        DwcTerm.nomenclaturalCode.qualifiedName(),
        "raw_er_" + DwcTerm.nomenclaturalCode.simpleName());
    core.put(
        DwcTerm.nomenclaturalStatus.qualifiedName(),
        "raw_er_" + DwcTerm.nomenclaturalStatus.simpleName());
    core.put(DwcTerm.organismID.qualifiedName(), "raw_er_" + DwcTerm.organismID.simpleName());
    core.put(
        DwcTerm.organismQuantity.qualifiedName(),
        "raw_er_" + DwcTerm.organismQuantity.simpleName());
    core.put(
        DwcTerm.organismQuantityType.qualifiedName(),
        "raw_er_" + DwcTerm.organismQuantityType.simpleName());
    core.put(
        DwcTerm.originalNameUsage.qualifiedName(),
        "raw_er_" + DwcTerm.originalNameUsage.simpleName());
    core.put(
        DwcTerm.originalNameUsageID.qualifiedName(),
        "raw_er_" + DwcTerm.originalNameUsageID.simpleName());
    core.put(
        DwcTerm.ownerInstitutionCode.qualifiedName(),
        "raw_er_" + DwcTerm.ownerInstitutionCode.simpleName());
    core.put(
        DwcTerm.parentNameUsage.qualifiedName(), "raw_er_" + DwcTerm.parentNameUsage.simpleName());
    core.put(
        DwcTerm.parentNameUsageID.qualifiedName(),
        "raw_er_" + DwcTerm.parentNameUsageID.simpleName());
    core.put(
        DwcTerm.pointRadiusSpatialFit.qualifiedName(),
        "raw_er_" + DwcTerm.pointRadiusSpatialFit.simpleName());
    core.put(DwcTerm.preparations.qualifiedName(), "raw_er_" + DwcTerm.preparations.simpleName());
    core.put(
        DwcTerm.previousIdentifications.qualifiedName(),
        "raw_er_" + DwcTerm.previousIdentifications.simpleName());
    core.put(
        DwcTerm.relatedResourceID.qualifiedName(),
        "raw_er_" + DwcTerm.relatedResourceID.simpleName());
    core.put(
        DwcTerm.relationshipAccordingTo.qualifiedName(),
        "raw_er_" + DwcTerm.relationshipAccordingTo.simpleName());
    core.put(
        DwcTerm.minimumDistanceAboveSurfaceInMeters.qualifiedName(),
        "raw_er_" + DwcTerm.minimumDistanceAboveSurfaceInMeters.simpleName());
    core.put(
        DwcTerm.relationshipEstablishedDate.qualifiedName(),
        "raw_er_" + DwcTerm.relationshipEstablishedDate.simpleName());
    core.put(
        DwcTerm.relationshipOfResource.qualifiedName(),
        "raw_er_" + DwcTerm.relationshipOfResource.simpleName());
    core.put(
        DwcTerm.relationshipRemarks.qualifiedName(),
        "raw_er_" + DwcTerm.relationshipRemarks.simpleName());
    core.put(
        DwcTerm.reproductiveCondition.qualifiedName(),
        "raw_er_" + DwcTerm.reproductiveCondition.simpleName());
    core.put(DwcTerm.resourceID.qualifiedName(), "raw_er_" + DwcTerm.resourceID.simpleName());
    core.put(
        DwcTerm.resourceRelationshipID.qualifiedName(),
        "raw_er_" + DwcTerm.resourceRelationshipID.simpleName());
    core.put(
        DwcTerm.samplingEffort.qualifiedName(), "raw_er_" + DwcTerm.samplingEffort.simpleName());
    core.put(
        DwcTerm.samplingProtocol.qualifiedName(),
        "raw_er_" + DwcTerm.samplingProtocol.simpleName());
    core.put(
        DwcTerm.scientificNameAuthorship.qualifiedName(),
        "raw_er_" + DwcTerm.scientificNameAuthorship.simpleName());
    core.put(
        DwcTerm.scientificNameID.qualifiedName(),
        "raw_er_" + DwcTerm.scientificNameID.simpleName());
    core.put(DwcTerm.sex.qualifiedName(), "raw_er_" + DwcTerm.sex.simpleName());
    core.put(
        DwcTerm.specificEpithet.qualifiedName(), "raw_er_" + DwcTerm.specificEpithet.simpleName());
    core.put(
        DwcTerm.startDayOfYear.qualifiedName(), "raw_er_" + DwcTerm.startDayOfYear.simpleName());
    core.put(DwcTerm.subgenus.qualifiedName(), "raw_er_" + DwcTerm.subgenus.simpleName());
    core.put(DwcTerm.taxonID.qualifiedName(), "raw_er_" + DwcTerm.taxonID.simpleName());
    core.put(
        DwcTerm.taxonomicStatus.qualifiedName(), "raw_er_" + DwcTerm.taxonomicStatus.simpleName());
    core.put(DwcTerm.taxonRemarks.qualifiedName(), "raw_er_" + DwcTerm.taxonRemarks.simpleName());
    core.put(DwcTerm.typeStatus.qualifiedName(), "raw_er_" + DwcTerm.typeStatus.simpleName());
    core.put(
        DwcTerm.verbatimCoordinates.qualifiedName(),
        "raw_er_" + DwcTerm.verbatimCoordinates.simpleName());
    core.put(
        DwcTerm.verbatimCoordinateSystem.qualifiedName(),
        "raw_er_" + DwcTerm.verbatimCoordinateSystem.simpleName());
    core.put(DwcTerm.verbatimDepth.qualifiedName(), "raw_er_" + DwcTerm.verbatimDepth.simpleName());
    core.put(
        DwcTerm.verbatimElevation.qualifiedName(),
        "raw_er_" + DwcTerm.verbatimElevation.simpleName());
    core.put(
        DwcTerm.verbatimEventDate.qualifiedName(),
        "raw_er_" + DwcTerm.verbatimEventDate.simpleName());
    core.put(
        DwcTerm.verbatimLatitude.qualifiedName(),
        "raw_er_" + DwcTerm.verbatimLatitude.simpleName());
    core.put(
        DwcTerm.verbatimLocality.qualifiedName(),
        "raw_er_" + DwcTerm.verbatimLocality.simpleName());
    core.put(
        DwcTerm.verbatimLongitude.qualifiedName(),
        "raw_er_" + DwcTerm.verbatimLongitude.simpleName());
    core.put(DwcTerm.verbatimSRS.qualifiedName(), "raw_er_" + DwcTerm.verbatimSRS.simpleName());
    core.put(
        DwcTerm.verbatimTaxonRank.qualifiedName(),
        "raw_er_" + DwcTerm.verbatimTaxonRank.simpleName());
    core.put(DwcTerm.waterBody.qualifiedName(), "raw_er_" + DwcTerm.waterBody.simpleName());
    core.put("http://rs.tdwg.org/dwc/terms/occurrenceAttributes", "raw_er_occurrenceAttributes");
    // DC Terms
    core.put(DcTerm.references.qualifiedName(), "raw_er_" + DcTerm.references.simpleName());
    core.put(DcTerm.accessRights.qualifiedName(), "raw_er_" + DcTerm.accessRights.simpleName());
    core.put(
        DcTerm.bibliographicCitation.qualifiedName(),
        "raw_er_" + DcTerm.bibliographicCitation.simpleName());
    core.put(DcTerm.language.qualifiedName(), "raw_er_" + DcTerm.language.simpleName());
    core.put(DcTerm.license.qualifiedName(), "raw_er_" + DcTerm.license.simpleName());
    core.put(DcTerm.modified.qualifiedName(), "raw_er_" + DcTerm.modified.simpleName());
    core.put(DcTerm.rights.qualifiedName(), "raw_er_" + DcTerm.rights.simpleName());
    core.put(DcTerm.rightsHolder.qualifiedName(), "raw_er_" + DcTerm.rightsHolder.simpleName());
    core.put(DcTerm.source.qualifiedName(), "raw_er_" + DcTerm.source.simpleName());
    core.put(DcTerm.type.qualifiedName(), "raw_er_" + DcTerm.type.simpleName());
    // ALA Terms
    core.put("http://rs.ala.org.au/terms/1.0/photographer", "raw_er_photographer");
    core.put("http://rs.ala.org.au/terms/1.0/northing", "raw_er_northing");
    core.put("http://rs.ala.org.au/terms/1.0/easting", "raw_er_easting");
    core.put("http://rs.ala.org.au/terms/1.0/species", "raw_er_species");
    core.put("http://rs.ala.org.au/terms/1.0/subfamily", "raw_er_subfamily");
    core.put("http://rs.ala.org.au/terms/1.0/subspecies", "raw_er_subspecies");
    core.put("http://rs.ala.org.au/terms/1.0/superfamily", "raw_er_superfamily");
    core.put("http://rs.ala.org.au/terms/1.0/zone", "raw_er_zone");
    // ABCD Terms
    core.put(
        "http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifier",
        "raw_er_abcdIdentificationQualifier");
    core.put(
        "http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifierInsertionPoint",
        "raw_er_abcdIdentificationQualifierInsertionPoint");
    core.put("http://rs.tdwg.org/abcd/terms/abcdTypeStatus", "raw_er_abcdTypeStatus");
    core.put("http://rs.tdwg.org/abcd/terms/typifiedName", "raw_er_typifiedName");
    // HISPID Terms
    core.put(
        "http://hiscom.chah.org.au/hispid/terms/secondaryCollectors", "raw_er_secondaryCollectors");
    core.put("http://hiscom.chah.org.au/hispid/terms/identifierRole", "raw_er_identifierRole");
    // GGBN Terms
    core.put("http://data.ggbn.org/schemas/ggbn/terms/loanDate", "raw_er_loanDate");
    core.put("http://data.ggbn.org/schemas/ggbn/terms/loanDestination", "raw_er_loanDestination");
    core.put("http://data.ggbn.org/schemas/ggbn/terms/loanIdentifier", "raw_er_loanIdentifier");
    core.put("http://rs.tdwg.org/dwc/terms/locationAttributes", "raw_er_locationAttributes");
    core.put("http://rs.tdwg.org/dwc/terms/eventAttributes", "raw_er_eventAttributes");
    // Other Terms
    core.put("taxonRankID", "raw_er_taxonRankID");
    // GBIF Terms
    core.put(DwcTerm.recordedByID.qualifiedName(), "raw_er_" + DwcTerm.recordedByID.simpleName());

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCoreTerms(core)
            .build();

    TemporalRecord tr =
        TemporalRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCreated(1L)
            .setDateIdentified("")
            .setCreated(11L)
            .setYear(111)
            .setMonth(1111)
            .setDay(11111)
            .setEventDate(EventDate.newBuilder().setGte("1999").setLte("2000").build())
            .setStartDayOfYear(111111)
            .setEndDayOfYear(1111111)
            .setModified("2001")
            .setDateIdentified("2002")
            .setDatePrecision("2003")
            .build();

    BasicRecord br =
        BasicRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCreated(2L)
            .setBasisOfRecord("br_basisOfRecord")
            .setSex("br_sex")
            .setLifeStage(
                VocabularyConcept.newBuilder()
                    .setConcept("br_lifeStage")
                    .setLineage(Collections.singletonList("br_lifeStageLineage"))
                    .build())
            .setEstablishmentMeans(
                VocabularyConcept.newBuilder()
                    .setConcept("br_establishmentMeans")
                    .setLineage(Collections.singletonList("br_establishmentMeans"))
                    .build())
            .setIndividualCount(222)
            .setTypeStatus(Collections.singletonList("br_typeStatus"))
            .setTypifiedName("br_typifiedName")
            .setSampleSizeValue(222d)
            .setSampleSizeUnit("br_sampleSizeUnit")
            .setOrganismQuantity(2222d)
            .setOrganismQuantityType("br_organismQuantityType")
            .setRelativeOrganismQuantity(22222d)
            .setReferences("br_References")
            .setLicense("br_license")
            .setIdentifiedByIds(
                Collections.singletonList(
                    AgentIdentifier.newBuilder()
                        .setType("br_agent_type")
                        .setValue("br_agent_value")
                        .build()))
            .setRecordedByIds(
                Collections.singletonList(
                    AgentIdentifier.newBuilder()
                        .setType("br_agent_type_rb")
                        .setValue("br_agent_value_rb")
                        .build()))
            .setRecordedBy(Arrays.asList("br_recordedBy_1", "br_recordedBy_2"))
            .setOccurrenceStatus("br_occurrenceStatus")
            .setDatasetID(Collections.singletonList("br_datasetID"))
            .setDatasetName(Collections.singletonList("br_datasetName"))
            .setOtherCatalogNumbers(Collections.singletonList("br_otherCatalogNumbers"))
            .setIdentifiedBy(Collections.singletonList("br_identifiedBy"))
            .setPreparations(Collections.singletonList("br_preparations"))
            .setSamplingProtocol(Collections.singletonList("br_samplingProtocol"))
            .build();

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setId("lr_id")
            .setCreated(3L)
            .setContinent("lr_continent")
            .setWaterBody("lr_waterBody")
            .setCountry("lr_country")
            .setCountryCode("lr_countryCode")
            .setPublishingCountry("lr_publishingCountry")
            .setStateProvince("lr_stateProvince")
            .setMinimumElevationInMeters(33d)
            .setMaximumElevationInMeters(333d)
            .setElevation(3333d)
            .setElevationAccuracy(33333d)
            .setMinimumDepthInMeters(33333d)
            .setMaximumDepthInMeters(333333d)
            .setDepth(33333333d)
            .setDepthAccuracy(333333333d)
            .setMinimumDistanceAboveSurfaceInMeters(3333333333d)
            .setMaximumDistanceAboveSurfaceInMeters(33333333333d)
            .setDecimalLatitude(333333333333d)
            .setDecimalLongitude(3333333333333d)
            .setCoordinateUncertaintyInMeters(33333333333333d)
            .setCoordinatePrecision(333333333333333d)
            .setHasCoordinate(true)
            .setRepatriated(true)
            .setHasGeospatialIssue(false)
            .setLocality("lr_locality")
            .setGeoreferencedDate("lr_georeferencedDate")
            .setFootprintWKT("lr_footprintWKT")
            .setBiome("lr_biome")
            .build();

    TaxonRecord txr =
        TaxonRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setSynonym(false)
            .setUsage(
                RankedName.newBuilder()
                    .setRank(Rank.SPECIES)
                    .setName("txr_Usage_name")
                    .setKey(4)
                    .build())
            .setClassification(
                Arrays.asList(
                    RankedName.newBuilder()
                        .setRank(Rank.SPECIES)
                        .setName("txr_Classification_SPECIES_name")
                        .setKey(44)
                        .build(),
                    RankedName.newBuilder()
                        .setRank(Rank.CLASS)
                        .setName("txr_Classification_CLASS_name")
                        .setKey(444)
                        .build()))
            .setAcceptedUsage(
                RankedName.newBuilder()
                    .setRank(Rank.SPECIES)
                    .setName("txr_Usage_name")
                    .setKey(4444)
                    .build())
            .setNomenclature(
                Nomenclature.newBuilder()
                    .setId("txr_Nomenclature_id")
                    .setSource("txr_Nomenclature_Source")
                    .build())
            .setDiagnostics(
                Diagnostic.newBuilder()
                    .setConfidence(44444)
                    .setStatus(Status.ACCEPTED)
                    .setNote("txr_Diagnostic_Note")
                    .setMatchType(MatchType.EXACT)
                    .setLineage(Collections.singletonList("txr_Diagnostic_Lineage"))
                    .build())
            .setUsageParsedName(ParsedName.newBuilder().build())
            .setIucnRedListCategoryCode("txr_IucnRedListCategoryCode")
            .build();

    ALATaxonRecord atxr =
        ALATaxonRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setScientificName("atxr_ScientificName")
            .setScientificNameAuthorship("atxr_ScientificNameAuthorship")
            .setTaxonConceptID("atxr_TaxonConceptID")
            .setTaxonRank("atxr_TaxonRank")
            .setTaxonRankID(5)
            .setLft(55)
            .setRgt(555)
            .setMatchType("atxr_MatchType")
            .setNameType("atxr_NameType")
            .setKingdom("atxr_Kingdom")
            .setKingdomID("atxr_KingdomID")
            .setPhylum("atxr_Phylum")
            .setPhylumID("atxr_PhylumID")
            .setClasss("atxr_Classs")
            .setClassID("atxr_ClassID")
            .setOrder("atxr_Order")
            .setOrderID("atxr_OrderID")
            .setFamily("atxr_Family")
            .setFamilyID("atxr_FamilyID")
            .setGenus("atxr_Genus")
            .setGenusID("atxr_GenusID")
            .setSpecies("atxr_Species")
            .setSpeciesID("atxr_SpeciesID")
            .setVernacularName("atxr_VernacularName")
            .setSpeciesGroup(Collections.singletonList("atxr_SpeciesGroup"))
            .setSpeciesSubgroup(Collections.singletonList("atxr_SpeciesSubgroup"))
            .build();

    ALAAttributionRecord aar =
        ALAAttributionRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setDataResourceUid("aar_DataResourceUid")
            .setDataResourceName("aar_DataResourceName")
            .setDataProviderUid("aar_DataProviderUid")
            .setDataProviderName("aar_DataProviderName")
            .setCollectionUid("aar_CollectionUid")
            .setCollectionName("aar_CollectionName")
            .setInstitutionUid("aar_InstitutionUid")
            .setInstitutionName("aar_InstitutionName")
            .setLicenseType("aar_LicenseType")
            .setLicenseVersion("aar_LicenseVersion")
            .setProvenance("aar_Provenance")
            .setHasDefaultValues(false)
            .setHubMembership(
                Collections.singletonList(
                    EntityReference.newBuilder()
                        .setName("aar_EntityReference_name")
                        .setUid("aar_EntityReference_uuid")
                        .setUri("aar_EntityReference_uri")
                        .build()))
            .build();

    ALAUUIDRecord aur =
        ALAUUIDRecord.newBuilder()
            .setId("aur_id")
            .setUuid("aur_uuid")
            .setUniqueKey("aur_uniqueKey")
            .setFirstLoaded(6L)
            .build();

    ImageRecord ir =
        ImageRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCreated(7L)
            .setImageItems(
                Collections.singletonList(
                    Image.newBuilder()
                        .setCreated("ir_Image")
                        .setAudience("ir_Audienc")
                        .setCreator("ir_Creator")
                        .setContributor("ir_Contributor")
                        .setDatasetId("ir_DatasetId")
                        .setLicense("ir_License")
                        .setLatitude(77d)
                        .setLongitude(777d)
                        .setSpatial("ir_Spatial")
                        .setTitle("ir_Title")
                        .setRightsHolder("ir_RightsHolder")
                        .setIdentifier("ir_Identifier")
                        .build()))
            .build();

    TaxonProfile tp = TaxonProfile.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    Multimedia m1 =
        Multimedia.newBuilder().setIdentifier("http://image.url/1").setLicense("CC-BY").build();
    Multimedia m2 =
        Multimedia.newBuilder().setIdentifier("http://image.url/2").setLicense("CC-BY-NC").build();

    MultimediaRecord mr =
        MultimediaRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setMultimediaItems(Arrays.asList(m1, m2))
            .build();

    ALASensitivityRecord asr =
        ALASensitivityRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCreated(8L)
            .setIsSensitive(false)
            .setSensitive("asr_Sensitive")
            .setDataGeneralizations("asr_DataGeneralizations")
            .setInformationWithheld("asr_InformationWithheld")
            .setGeneralisationToApplyInMetres("asr_GeneralisationToApplyInMetres")
            .setGeneralisationInMetres("asr_GeneralisationInMetres")
            .setOriginal(Collections.singletonMap("asr_Original_key", "asr_Original_value"))
            .setAltered(Collections.singletonMap("asr_Altered_key", "asr_Altered_value"))
            .build();

    Long lastLoadDate = 9L;
    Long lastProcessedDate = 10L;

    IndexRecord source =
        IndexRecordTransform.createIndexRecord(
            br,
            tr,
            lr,
            txr,
            atxr,
            er,
            aar,
            aur,
            ir,
            tp,
            asr,
            mr,
            null,
            null,
            null,
            lastLoadDate,
            lastProcessedDate);

    // When
    String result = CoreCsvConverter.convert(source);

    // Should
    Assert.assertEquals(String.join("\t", expected), result);
  }

  @Test
  public void converterDefaultTest() {
    // Expected
    // tr = 1, br = 2, lr = 3, trx = 4, atxr = 5, aur = 6, ir = 7, asr = 8
    String[] expected = {
      // DWC Terms
      "\"aur_uuid\"", // DwcTerm.occurrenceID
      "\"raw_er_catalogNumber\"", // DwcTerm.catalogNumber
      "\"raw_er_collectionCode\"", // DwcTerm.collectionCode
      "\"raw_er_institutionCode\"", // DwcTerm.institutionCode
      "\"raw_er_recordNumber\"", // DwcTerm.recordNumber
      "\"HumanObservation\"", // DwcTerm.basisOfRecord
      "\"\"", // DwcTerm.recordedBy
      "\"\"", // DwcTerm.occurrenceStatus
      "\"\"", // DwcTerm.individualCount
      "\"\"", // DwcTerm.scientificName
      "\"\"", // DwcTerm.taxonConceptID
      "\"\"", // DwcTerm.taxonRank
      "\"\"", // DwcTerm.kingdom
      "\"\"", // DwcTerm.phylum
      "\"\"", // DwcTerm.class_
      "\"\"", // DwcTerm.order
      "\"\"", // DwcTerm.family
      "\"\"", // DwcTerm.genus
      "\"\"", // DwcTerm.vernacularName
      "\"\"", // DwcTerm.decimalLatitude
      "\"\"", // DwcTerm.decimalLongitude
      "\"\"", // DwcTerm.geodeticDatum
      "\"\"", // DwcTerm.coordinateUncertaintyInMeters
      "\"\"", // DwcTerm.maximumElevationInMeters
      "\"\"", // DwcTerm.minimumElevationInMeters
      "\"\"", // DwcTerm.minimumDepthInMeters
      "\"\"", // DwcTerm.maximumDepthInMeters
      "\"\"", // DwcTerm.country
      "\"\"", // DwcTerm.stateProvince
      "\"\"", // DwcTerm.locality
      "\"raw_er_locationRemarks\"", // DwcTerm.locationRemarks
      "\"\"", // DwcTerm.year
      "\"\"", // DwcTerm.month
      "\"\"", // DwcTerm.day
      "\"raw_er_eventDate\"", // DwcTerm.eventDate
      "\"raw_er_eventID\"", // DwcTerm.eventID
      "\"\"", // DwcTerm.identifiedBy
      "\"raw_er_occurrenceRemarks\"", // DwcTerm.occurrenceRemarks
      "\"\"", // DwcTerm.dataGeneralizations
      "\"\"", // DwcTerm.otherCatalogNumbers
      "\"\"", // DwcTerm.acceptedNameUsage
      "\"\"", // DwcTerm.acceptedNameUsageID
      "\"\"", // DwcTerm.associatedOccurrences
      "\"\"", // DwcTerm.associatedReferences
      "\"\"", // DwcTerm.associatedSequences
      "\"\"", // DwcTerm.associatedTaxa
      "\"\"", // DwcTerm.behavior
      "\"\"", // DwcTerm.collectionID
      "\"\"", // DwcTerm.continent
      "\"\"", // DwcTerm.coordinatePrecision
      "\"\"", // DwcTerm.countryCode
      "\"\"", // DwcTerm.county
      "\"\"", // DwcTerm.datasetID
      "\"\"", // DwcTerm.datasetName
      "\"\"", // DwcTerm.dateIdentified
      "\"\"", // DwcTerm.disposition
      "\"\"", // DwcTerm.dynamicProperties
      "\"\"", // DwcTerm.endDayOfYear
      "\"\"", // DwcTerm.establishmentMeans
      "\"\"", // DwcTerm.eventRemarks
      "\"\"", // DwcTerm.eventTime
      "\"\"", // DwcTerm.fieldNotes
      "\"\"", // DwcTerm.fieldNumber
      "\"\"", // DwcTerm.footprintSpatialFit
      "\"\"", // DwcTerm.footprintSRS
      "\"\"", // DwcTerm.footprintWKT
      "\"\"", // DwcTerm.georeferencedBy
      "\"\"", // DwcTerm.georeferencedDate
      "\"\"", // DwcTerm.georeferenceProtocol
      "\"\"", // DwcTerm.georeferenceRemarks
      "\"\"", // DwcTerm.georeferenceSources
      "\"\"", // DwcTerm.georeferenceVerificationStatus
      "\"\"", // DwcTerm.habitat
      "\"\"", // DwcTerm.higherClassification
      "\"\"", // DwcTerm.higherGeography
      "\"\"", // DwcTerm.higherGeographyID
      "\"\"", // DwcTerm.identificationID
      "\"\"", // DwcTerm.identificationQualifier
      "\"\"", // DwcTerm.identificationReferences
      "\"\"", // DwcTerm.identificationRemarks
      "\"\"", // DwcTerm.identificationVerificationStatus
      "\"\"", // DwcTerm.informationWithheld
      "\"\"", // DwcTerm.infraspecificEpithet
      "\"\"", // DwcTerm.institutionID
      "\"\"", // DwcTerm.island
      "\"\"", // DwcTerm.islandGroup
      "\"\"", // DwcTerm.lifeStage
      "\"\"", // DwcTerm.locationAccordingTo
      "\"\"", // DwcTerm.locationID
      "\"\"", // DwcTerm.maximumDistanceAboveSurfaceInMeters
      "\"\"", // DwcTerm.measurementAccuracy
      "\"\"", // DwcTerm.measurementDeterminedBy
      "\"\"", // DwcTerm.measurementDeterminedDate
      "\"\"", // DwcTerm.measurementID
      "\"\"", // DwcTerm.measurementMethod
      "\"\"", // DwcTerm.measurementRemarks
      "\"\"", // DwcTerm.measurementType
      "\"\"", // DwcTerm.measurementUnit
      "\"\"", // DwcTerm.measurementValue
      "\"\"", // DwcTerm.municipality
      "\"\"", // DwcTerm.nameAccordingTo
      "\"\"", // DwcTerm.nameAccordingToID
      "\"\"", // DwcTerm.namePublishedIn
      "\"\"", // DwcTerm.namePublishedInID
      "\"\"", // DwcTerm.namePublishedInYear
      "\"\"", // DwcTerm.nomenclaturalCode
      "\"\"", // DwcTerm.nomenclaturalStatus
      "\"\"", // DwcTerm.organismID
      "\"\"", // DwcTerm.organismQuantity
      "\"\"", // DwcTerm.organismQuantityType
      "\"\"", // DwcTerm.originalNameUsage
      "\"\"", // DwcTerm.originalNameUsageID
      "\"\"", // DwcTerm.ownerInstitutionCode
      "\"\"", // DwcTerm.parentNameUsage
      "\"\"", // DwcTerm.parentNameUsageID
      "\"\"", // DwcTerm.pointRadiusSpatialFit
      "\"\"", // DwcTerm.preparations
      "\"\"", // DwcTerm.previousIdentifications
      "\"\"", // DwcTerm.relatedResourceID
      "\"\"", // DwcTerm.relationshipAccordingTo
      "\"\"", // DwcTerm.minimumDistanceAboveSurfaceInMeters
      "\"\"", // DwcTerm.relationshipEstablishedDate
      "\"\"", // DwcTerm.relationshipOfResource
      "\"\"", // DwcTerm.relationshipRemarks
      "\"\"", // DwcTerm.reproductiveCondition
      "\"\"", // DwcTerm.resourceID
      "\"\"", // DwcTerm.resourceRelationshipID
      "\"\"", // DwcTerm.samplingEffort
      "\"\"", // DwcTerm.samplingProtocol
      "\"\"", // DwcTerm.scientificNameAuthorship
      "\"\"", // DwcTerm.scientificNameID
      "\"\"", // DwcTerm.sex
      "\"\"", // DwcTerm.specificEpithet
      "\"\"", // DwcTerm.startDayOfYear
      "\"\"", // DwcTerm.subgenus
      "\"\"", // DwcTerm.taxonID
      "\"\"", // DwcTerm.taxonomicStatus
      "\"\"", // DwcTerm.taxonRemarks
      "\"\"", // DwcTerm.typeStatus
      "\"\"", // DwcTerm.verbatimCoordinates
      "\"\"", // DwcTerm.verbatimCoordinateSystem
      "\"\"", // DwcTerm.verbatimDepth
      "\"\"", // DwcTerm.verbatimElevation
      "\"\"", // DwcTerm.verbatimEventDate
      "\"\"", // DwcTerm.verbatimLatitude
      "\"\"", // DwcTerm.verbatimLocality
      "\"\"", // DwcTerm.verbatimLongitude
      "\"\"", // DwcTerm.verbatimSRS
      "\"\"", // DwcTerm.verbatimTaxonRank
      "\"\"", // DwcTerm.waterBody
      "\"\"", // http://rs.tdwg.org/dwc/terms/occurrenceAttributes
      // DC Terms
      "\"\"", // DcTerm.references
      "\"\"", // DcTerm.accessRights
      "\"\"", // DcTerm.bibliographicCitation
      "\"\"", // DcTerm.language
      "\"\"", // DcTerm.license
      "\"\"", // DcTerm.modified
      "\"\"", // DcTerm.rights
      "\"\"", // DcTerm.rightsHolder
      "\"\"", // DcTerm.source
      "\"\"", // DcTerm.type
      // ALA Terms
      "\"\"", // http://rs.ala.org.au/terms/1.0/photographer
      "\"\"", // http://rs.ala.org.au/terms/1.0/northing
      "\"\"", // http://rs.ala.org.au/terms/1.0/easting
      "\"\"", // http://rs.ala.org.au/terms/1.0/species
      "\"\"", // http://rs.ala.org.au/terms/1.0/subfamily
      "\"\"", // http://rs.ala.org.au/terms/1.0/subspecies
      "\"\"", // http://rs.ala.org.au/terms/1.0/superfamily
      "\"\"", // http://rs.ala.org.au/terms/1.0/zone
      // ABCD Terms
      "\"\"", // http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifier
      "\"\"", // http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifierInsertionPoint
      "\"\"", // http://rs.tdwg.org/abcd/terms/abcdTypeStatus
      "\"\"", // http://rs.tdwg.org/abcd/terms/typifiedName
      // HISPID Terms
      "\"\"", // http://hiscom.chah.org.au/hispid/terms/secondaryCollectors
      "\"\"", // http://hiscom.chah.org.au/hispid/terms/identifierRole
      // GGBN Terms
      "\"\"", // http://data.ggbn.org/schemas/ggbn/terms/loanDate
      "\"\"", // http://data.ggbn.org/schemas/ggbn/terms/loanDestination
      "\"\"", // http://data.ggbn.org/schemas/ggbn/terms/loanIdentifier
      "\"\"", // http://rs.tdwg.org/dwc/terms/locationAttributes
      "\"\"", // http://rs.tdwg.org/dwc/terms/eventAttributes
      // Other Terms
      "\"\"", // taxonRankID
      // GBIF Terms
      "\"\"" // GbifTerm.recordedByID
    };

    // State
    Map<String, String> core = new HashMap<>();
    core.put(DwcTerm.occurrenceID.simpleName(), "raw_er_" + DwcTerm.occurrenceID.simpleName());
    core.put(DwcTerm.catalogNumber.simpleName(), "raw_er_" + DwcTerm.catalogNumber.simpleName());
    core.put(DwcTerm.collectionCode.simpleName(), "raw_er_" + DwcTerm.collectionCode.simpleName());
    core.put(
        DwcTerm.institutionCode.simpleName(), "raw_er_" + DwcTerm.institutionCode.simpleName());
    core.put(DwcTerm.recordNumber.simpleName(), "raw_er_" + DwcTerm.recordNumber.simpleName());
    core.put(DwcTerm.basisOfRecord.simpleName(), "raw_er_" + DwcTerm.basisOfRecord.simpleName());
    core.put(DwcTerm.recordedBy.simpleName(), "raw_er_" + DwcTerm.recordedBy.simpleName());
    core.put(
        DwcTerm.occurrenceStatus.simpleName(), "raw_er_" + DwcTerm.occurrenceStatus.simpleName());
    core.put(
        DwcTerm.individualCount.simpleName(), "raw_er_" + DwcTerm.individualCount.simpleName());
    core.put(DwcTerm.scientificName.simpleName(), "raw_er_" + DwcTerm.scientificName.simpleName());
    core.put(DwcTerm.taxonConceptID.simpleName(), "raw_er_" + DwcTerm.taxonConceptID.simpleName());
    core.put(DwcTerm.taxonRank.simpleName(), "raw_er_" + DwcTerm.taxonRank.simpleName());
    core.put(DwcTerm.kingdom.simpleName(), "raw_er_" + DwcTerm.kingdom.simpleName());
    core.put(DwcTerm.phylum.simpleName(), "raw_er_" + DwcTerm.phylum.simpleName());
    core.put(DwcTerm.class_.simpleName(), "raw_er_" + DwcTerm.class_.simpleName());
    core.put(DwcTerm.order.simpleName(), "raw_er_" + DwcTerm.order.simpleName());
    core.put(DwcTerm.family.simpleName(), "raw_er_" + DwcTerm.family.simpleName());
    core.put(DwcTerm.genus.simpleName(), "raw_er_" + DwcTerm.genus.simpleName());
    core.put(DwcTerm.vernacularName.simpleName(), "raw_er_" + DwcTerm.vernacularName.simpleName());
    core.put(
        DwcTerm.decimalLatitude.simpleName(), "raw_er_" + DwcTerm.decimalLatitude.simpleName());
    core.put(
        DwcTerm.decimalLongitude.simpleName(), "raw_er_" + DwcTerm.decimalLongitude.simpleName());
    core.put(DwcTerm.geodeticDatum.simpleName(), "raw_er_" + DwcTerm.geodeticDatum.simpleName());
    core.put(
        DwcTerm.coordinateUncertaintyInMeters.simpleName(),
        "raw_er_" + DwcTerm.coordinateUncertaintyInMeters.simpleName());
    core.put(
        DwcTerm.maximumElevationInMeters.simpleName(),
        "raw_er_" + DwcTerm.maximumElevationInMeters.simpleName());
    core.put(
        DwcTerm.minimumElevationInMeters.simpleName(),
        "raw_er_" + DwcTerm.minimumElevationInMeters.simpleName());
    core.put(
        DwcTerm.minimumDepthInMeters.simpleName(),
        "raw_er_" + DwcTerm.minimumDepthInMeters.simpleName());
    core.put(
        DwcTerm.maximumDepthInMeters.simpleName(),
        "raw_er_" + DwcTerm.maximumDepthInMeters.simpleName());
    core.put(DwcTerm.country.simpleName(), "raw_er_" + DwcTerm.country.simpleName());
    core.put(DwcTerm.stateProvince.simpleName(), "raw_er_" + DwcTerm.stateProvince.simpleName());
    core.put(DwcTerm.locality.simpleName(), "raw_er_" + DwcTerm.locality.simpleName());
    core.put(
        DwcTerm.locationRemarks.simpleName(), "raw_er_" + DwcTerm.locationRemarks.simpleName());
    core.put(DwcTerm.year.simpleName(), "raw_er_" + DwcTerm.year.simpleName());
    core.put(DwcTerm.month.simpleName(), "raw_er_" + DwcTerm.month.simpleName());
    core.put(DwcTerm.day.simpleName(), "raw_er_" + DwcTerm.day.simpleName());
    core.put(DwcTerm.eventDate.simpleName(), "raw_er_" + DwcTerm.eventDate.simpleName());
    core.put(DwcTerm.eventID.simpleName(), "raw_er_" + DwcTerm.eventID.simpleName());
    core.put(DwcTerm.identifiedBy.simpleName(), "raw_er_" + DwcTerm.identifiedBy.simpleName());
    core.put(
        DwcTerm.occurrenceRemarks.simpleName(), "raw_er_" + DwcTerm.occurrenceRemarks.simpleName());
    core.put(
        DwcTerm.dataGeneralizations.simpleName(),
        "raw_er_" + DwcTerm.dataGeneralizations.simpleName());
    core.put(
        DwcTerm.otherCatalogNumbers.simpleName(),
        "raw_er_" + DwcTerm.otherCatalogNumbers.simpleName());
    core.put(DcTerm.references.simpleName(), "raw_er_" + DcTerm.references.simpleName());

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCoreTerms(core)
            .build();

    TemporalRecord tr =
        TemporalRecord.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    BasicRecord br = BasicRecord.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    LocationRecord lr =
        LocationRecord.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    TaxonRecord txr =
        TaxonRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setAcceptedUsage(
                RankedName.newBuilder()
                    .setRank(Rank.SPECIES)
                    .setName("txr_Usage_name")
                    .setKey(4444)
                    .build())
            .build();

    ALATaxonRecord atxr =
        ALATaxonRecord.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    ALAAttributionRecord aar =
        ALAAttributionRecord.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    ALAUUIDRecord aur =
        ALAUUIDRecord.newBuilder()
            .setId("aur_id")
            .setUuid("aur_uuid")
            .setUniqueKey("aur_uniqueKey")
            .setFirstLoaded(6L)
            .build();

    ImageRecord ir = ImageRecord.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    TaxonProfile tp = TaxonProfile.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    MultimediaRecord mr =
        MultimediaRecord.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    ALASensitivityRecord asr =
        ALASensitivityRecord.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    Long lastLoadDate = 9L;
    Long lastProcessedDate = 10L;

    IndexRecord source =
        IndexRecordTransform.createIndexRecord(
            br,
            tr,
            lr,
            txr,
            atxr,
            er,
            aar,
            aur,
            ir,
            tp,
            asr,
            mr,
            null,
            null,
            null,
            lastLoadDate,
            lastProcessedDate);

    // When
    String result = CoreCsvConverter.convert(source);

    System.out.println("### EXPECTED " + String.join("\t", expected));
    System.out.println("##### RESULT " + result);

    // Should
    Assert.assertEquals(String.join("\t", expected), result);
  }

  @Test
  public void termListTest() {

    // Expected
    List<String> expected = new LinkedList<>();
    expected.add(DwcTerm.occurrenceID.qualifiedName());
    expected.add(DwcTerm.catalogNumber.qualifiedName());
    expected.add(DwcTerm.collectionCode.qualifiedName());
    expected.add(DwcTerm.institutionCode.qualifiedName());
    expected.add(DwcTerm.recordNumber.qualifiedName());
    expected.add(DwcTerm.basisOfRecord.qualifiedName());
    expected.add(DwcTerm.recordedBy.qualifiedName());
    expected.add(DwcTerm.occurrenceStatus.qualifiedName());
    expected.add(DwcTerm.individualCount.qualifiedName());
    expected.add(DwcTerm.scientificName.qualifiedName());
    expected.add(DwcTerm.taxonConceptID.qualifiedName());
    expected.add(DwcTerm.taxonRank.qualifiedName());
    expected.add(DwcTerm.kingdom.qualifiedName());
    expected.add(DwcTerm.phylum.qualifiedName());
    expected.add(DwcTerm.class_.qualifiedName());
    expected.add(DwcTerm.order.qualifiedName());
    expected.add(DwcTerm.family.qualifiedName());
    expected.add(DwcTerm.genus.qualifiedName());
    expected.add(DwcTerm.vernacularName.qualifiedName());
    expected.add(DwcTerm.decimalLatitude.qualifiedName());
    expected.add(DwcTerm.decimalLongitude.qualifiedName());
    expected.add(DwcTerm.geodeticDatum.qualifiedName());
    expected.add(DwcTerm.coordinateUncertaintyInMeters.qualifiedName());
    expected.add(DwcTerm.maximumElevationInMeters.qualifiedName());
    expected.add(DwcTerm.minimumElevationInMeters.qualifiedName());
    expected.add(DwcTerm.minimumDepthInMeters.qualifiedName());
    expected.add(DwcTerm.maximumDepthInMeters.qualifiedName());
    expected.add(DwcTerm.country.qualifiedName());
    expected.add(DwcTerm.stateProvince.qualifiedName());
    expected.add(DwcTerm.locality.qualifiedName());
    expected.add(DwcTerm.locationRemarks.qualifiedName());
    expected.add(DwcTerm.year.qualifiedName());
    expected.add(DwcTerm.month.qualifiedName());
    expected.add(DwcTerm.day.qualifiedName());
    expected.add(DwcTerm.eventDate.qualifiedName());
    expected.add(DwcTerm.eventID.qualifiedName());
    expected.add(DwcTerm.identifiedBy.qualifiedName());
    expected.add(DwcTerm.occurrenceRemarks.qualifiedName());
    expected.add(DwcTerm.dataGeneralizations.qualifiedName());
    expected.add(DwcTerm.otherCatalogNumbers.qualifiedName());
    expected.add(DwcTerm.acceptedNameUsage.qualifiedName());
    expected.add(DwcTerm.acceptedNameUsageID.qualifiedName());
    expected.add(DwcTerm.associatedOccurrences.qualifiedName());
    expected.add(DwcTerm.associatedReferences.qualifiedName());
    expected.add(DwcTerm.associatedSequences.qualifiedName());
    expected.add(DwcTerm.associatedTaxa.qualifiedName());
    expected.add(DwcTerm.behavior.qualifiedName());
    expected.add(DwcTerm.collectionID.qualifiedName());
    expected.add(DwcTerm.continent.qualifiedName());
    expected.add(DwcTerm.coordinatePrecision.qualifiedName());
    expected.add(DwcTerm.countryCode.qualifiedName());
    expected.add(DwcTerm.county.qualifiedName());
    expected.add(DwcTerm.datasetID.qualifiedName());
    expected.add(DwcTerm.datasetName.qualifiedName());
    expected.add(DwcTerm.dateIdentified.qualifiedName());
    expected.add(DwcTerm.disposition.qualifiedName());
    expected.add(DwcTerm.dynamicProperties.qualifiedName());
    expected.add(DwcTerm.endDayOfYear.qualifiedName());
    expected.add(DwcTerm.establishmentMeans.qualifiedName());
    expected.add(DwcTerm.eventRemarks.qualifiedName());
    expected.add(DwcTerm.eventTime.qualifiedName());
    expected.add(DwcTerm.fieldNotes.qualifiedName());
    expected.add(DwcTerm.fieldNumber.qualifiedName());
    expected.add(DwcTerm.footprintSpatialFit.qualifiedName());
    expected.add(DwcTerm.footprintSRS.qualifiedName());
    expected.add(DwcTerm.footprintWKT.qualifiedName());
    expected.add(DwcTerm.georeferencedBy.qualifiedName());
    expected.add(DwcTerm.georeferencedDate.qualifiedName());
    expected.add(DwcTerm.georeferenceProtocol.qualifiedName());
    expected.add(DwcTerm.georeferenceRemarks.qualifiedName());
    expected.add(DwcTerm.georeferenceSources.qualifiedName());
    expected.add(DwcTerm.georeferenceVerificationStatus.qualifiedName());
    expected.add(DwcTerm.habitat.qualifiedName());
    expected.add(DwcTerm.higherClassification.qualifiedName());
    expected.add(DwcTerm.higherGeography.qualifiedName());
    expected.add(DwcTerm.higherGeographyID.qualifiedName());
    expected.add(DwcTerm.identificationID.qualifiedName());
    expected.add(DwcTerm.identificationQualifier.qualifiedName());
    expected.add(DwcTerm.identificationReferences.qualifiedName());
    expected.add(DwcTerm.identificationRemarks.qualifiedName());
    expected.add(DwcTerm.identificationVerificationStatus.qualifiedName());
    expected.add(DwcTerm.informationWithheld.qualifiedName());
    expected.add(DwcTerm.infraspecificEpithet.qualifiedName());
    expected.add(DwcTerm.institutionID.qualifiedName());
    expected.add(DwcTerm.island.qualifiedName());
    expected.add(DwcTerm.islandGroup.qualifiedName());
    expected.add(DwcTerm.lifeStage.qualifiedName());
    expected.add(DwcTerm.locationAccordingTo.qualifiedName());
    expected.add(DwcTerm.locationID.qualifiedName());
    expected.add(DwcTerm.maximumDistanceAboveSurfaceInMeters.qualifiedName());
    expected.add(DwcTerm.measurementAccuracy.qualifiedName());
    expected.add(DwcTerm.measurementDeterminedBy.qualifiedName());
    expected.add(DwcTerm.measurementDeterminedDate.qualifiedName());
    expected.add(DwcTerm.measurementID.qualifiedName());
    expected.add(DwcTerm.measurementMethod.qualifiedName());
    expected.add(DwcTerm.measurementRemarks.qualifiedName());
    expected.add(DwcTerm.measurementType.qualifiedName());
    expected.add(DwcTerm.measurementUnit.qualifiedName());
    expected.add(DwcTerm.measurementValue.qualifiedName());
    expected.add(DwcTerm.municipality.qualifiedName());
    expected.add(DwcTerm.nameAccordingTo.qualifiedName());
    expected.add(DwcTerm.nameAccordingToID.qualifiedName());
    expected.add(DwcTerm.namePublishedIn.qualifiedName());
    expected.add(DwcTerm.namePublishedInID.qualifiedName());
    expected.add(DwcTerm.namePublishedInYear.qualifiedName());
    expected.add(DwcTerm.nomenclaturalCode.qualifiedName());
    expected.add(DwcTerm.nomenclaturalStatus.qualifiedName());
    expected.add(DwcTerm.organismID.qualifiedName());
    expected.add(DwcTerm.organismQuantity.qualifiedName());
    expected.add(DwcTerm.organismQuantityType.qualifiedName());
    expected.add(DwcTerm.originalNameUsage.qualifiedName());
    expected.add(DwcTerm.originalNameUsageID.qualifiedName());
    expected.add(DwcTerm.ownerInstitutionCode.qualifiedName());
    expected.add(DwcTerm.parentNameUsage.qualifiedName());
    expected.add(DwcTerm.parentNameUsageID.qualifiedName());
    expected.add(DwcTerm.pointRadiusSpatialFit.qualifiedName());
    expected.add(DwcTerm.preparations.qualifiedName());
    expected.add(DwcTerm.previousIdentifications.qualifiedName());
    expected.add(DwcTerm.relatedResourceID.qualifiedName());
    expected.add(DwcTerm.relationshipAccordingTo.qualifiedName());
    expected.add(DwcTerm.minimumDistanceAboveSurfaceInMeters.qualifiedName());
    expected.add(DwcTerm.relationshipEstablishedDate.qualifiedName());
    expected.add(DwcTerm.relationshipOfResource.qualifiedName());
    expected.add(DwcTerm.relationshipRemarks.qualifiedName());
    expected.add(DwcTerm.reproductiveCondition.qualifiedName());
    expected.add(DwcTerm.resourceID.qualifiedName());
    expected.add(DwcTerm.resourceRelationshipID.qualifiedName());
    expected.add(DwcTerm.samplingEffort.qualifiedName());
    expected.add(DwcTerm.samplingProtocol.qualifiedName());
    expected.add(DwcTerm.scientificNameAuthorship.qualifiedName());
    expected.add(DwcTerm.scientificNameID.qualifiedName());
    expected.add(DwcTerm.sex.qualifiedName());
    expected.add(DwcTerm.specificEpithet.qualifiedName());
    expected.add(DwcTerm.startDayOfYear.qualifiedName());
    expected.add(DwcTerm.subgenus.qualifiedName());
    expected.add(DwcTerm.taxonID.qualifiedName());
    expected.add(DwcTerm.taxonomicStatus.qualifiedName());
    expected.add(DwcTerm.taxonRemarks.qualifiedName());
    expected.add(DwcTerm.typeStatus.qualifiedName());
    expected.add(DwcTerm.verbatimCoordinates.qualifiedName());
    expected.add(DwcTerm.verbatimCoordinateSystem.qualifiedName());
    expected.add(DwcTerm.verbatimDepth.qualifiedName());
    expected.add(DwcTerm.verbatimElevation.qualifiedName());
    expected.add(DwcTerm.verbatimEventDate.qualifiedName());
    expected.add(DwcTerm.verbatimLatitude.qualifiedName());
    expected.add(DwcTerm.verbatimLocality.qualifiedName());
    expected.add(DwcTerm.verbatimLongitude.qualifiedName());
    expected.add(DwcTerm.verbatimSRS.qualifiedName());
    expected.add(DwcTerm.verbatimTaxonRank.qualifiedName());
    expected.add(DwcTerm.waterBody.qualifiedName());
    expected.add("http://rs.tdwg.org/dwc/terms/occurrenceAttributes");
    // DC Terms
    expected.add(DcTerm.references.qualifiedName());
    expected.add(DcTerm.accessRights.qualifiedName());
    expected.add(DcTerm.bibliographicCitation.qualifiedName());
    expected.add(DcTerm.language.qualifiedName());
    expected.add(DcTerm.license.qualifiedName());
    expected.add(DcTerm.modified.qualifiedName());
    expected.add(DcTerm.rights.qualifiedName());
    expected.add(DcTerm.rightsHolder.qualifiedName());
    expected.add(DcTerm.source.qualifiedName());
    expected.add(DcTerm.type.qualifiedName());
    // ALA Terms
    expected.add("http://rs.ala.org.au/terms/1.0/photographer");
    expected.add("http://rs.ala.org.au/terms/1.0/northing");
    expected.add("http://rs.ala.org.au/terms/1.0/easting");
    expected.add("http://rs.ala.org.au/terms/1.0/species");
    expected.add("http://rs.ala.org.au/terms/1.0/subfamily");
    expected.add("http://rs.ala.org.au/terms/1.0/subspecies");
    expected.add("http://rs.ala.org.au/terms/1.0/superfamily");
    expected.add("http://rs.ala.org.au/terms/1.0/zone");
    // ABCD Terms
    expected.add("http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifier");
    expected.add("http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifierInsertionPoint");
    expected.add("http://rs.tdwg.org/abcd/terms/abcdTypeStatus");
    expected.add("http://rs.tdwg.org/abcd/terms/typifiedName");
    // HISPID Terms
    expected.add("http://hiscom.chah.org.au/hispid/terms/secondaryCollectors");
    expected.add("http://hiscom.chah.org.au/hispid/terms/identifierRole");
    // GGBN Terms
    expected.add("http://data.ggbn.org/schemas/ggbn/terms/loanDate");
    expected.add("http://data.ggbn.org/schemas/ggbn/terms/loanDestination");
    expected.add("http://data.ggbn.org/schemas/ggbn/terms/loanIdentifier");
    expected.add("http://rs.tdwg.org/dwc/terms/locationAttributes");
    expected.add("http://rs.tdwg.org/dwc/terms/eventAttributes");
    // Other Terms
    expected.add("taxonRankID");
    // GBIF Terms
    expected.add(DwcTerm.recordedByID.qualifiedName());

    // When
    List<String> result = CoreCsvConverter.getTerms();

    // Should
    Assert.assertEquals(expected.size(), result.size());
    for (int x = 0; x < expected.size(); x++) {
      Assert.assertEquals(expected.get(x), result.get(x));
    }
  }
}
