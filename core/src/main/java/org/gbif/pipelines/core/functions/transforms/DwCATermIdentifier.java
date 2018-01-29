package org.gbif.pipelines.core.functions.transforms;

/**
 * This enum helps in identifying the DwCATerms with its identifier
 */
public enum DwCATermIdentifier {
  Occurrence("http://rs.tdwg.org/dwc/terms/Occurrence"), Organism("http://rs.tdwg.org/dwc/terms/Organism"), MaterialSample(
    "http://rs.tdwg.org/dwc/terms/MaterialSample"), LivingSpecimen("http://rs.tdwg.org/dwc/terms/LivingSpecimen"), PreservedSpecimen(
    "http://rs.tdwg.org/dwc/terms/PreservedSpecimen"), FossilSpecimen("http://rs.tdwg.org/dwc/terms/FossilSpecimen"), Event(
    "http://rs.tdwg.org/dwc/terms/Event"), HumanObservation("http://rs.tdwg.org/dwc/terms/HumanObservation"), MachineObservation(
    "http://rs.tdwg.org/dwc/terms/MachineObservation"), Location("http://purl.org/dc/terms/Location"), GeologicalContext(
    "http://rs.tdwg.org/dwc/terms/GeologicalContext"), Identification("http://rs.tdwg.org/dwc/terms/Identification"), Taxon(
    "http://rs.tdwg.org/dwc/terms/Taxon"), MeasurementOrFact("http://rs.tdwg.org/dwc/terms/MeasurementOrFact"), ResourceRelationship(
    "http://rs.tdwg.org/dwc/terms/ResourceRelationship"), dcterms_type("http://purl.org/dc/terms/type"), dcterms_modified(
    "http://purl.org/dc/terms/modified"), dcterms_language("http://purl.org/dc/terms/language"), dcterms_license(
    "http://purl.org/dc/terms/license"), dcterms_rightsHolder("http://purl.org/dc/terms/rightsHolder"), dcterms_accessRights(
    "http://purl.org/dc/terms/accessRights"), dcterms_bibliographicCitation(
    "http://purl.org/dc/terms/bibliographicCitation"), dcterms_references("http://purl.org/dc/terms/references"), institutionID(
    "http://rs.tdwg.org/dwc/terms/institutionID"), collectionID("http://rs.tdwg.org/dwc/terms/collectionID"), datasetID(
    "http://rs.tdwg.org/dwc/terms/datasetID"), institutionCode("http://rs.tdwg.org/dwc/terms/institutionCode"), collectionCode(
    "http://rs.tdwg.org/dwc/terms/collectionCode"), datasetName("http://rs.tdwg.org/dwc/terms/datasetName"), ownerInstitutionCode(
    "http://rs.tdwg.org/dwc/terms/ownerInstitutionCode"), basisOfRecord("http://rs.tdwg.org/dwc/terms/basisOfRecord"), informationWithheld(
    "http://rs.tdwg.org/dwc/terms/informationWithheld"), dataGeneralizations(
    "http://rs.tdwg.org/dwc/terms/dataGeneralizations"), dynamicProperties(
    "http://rs.tdwg.org/dwc/terms/dynamicProperties"), occurrenceID("http://rs.tdwg.org/dwc/terms/occurrenceID"), catalogNumber(
    "http://rs.tdwg.org/dwc/terms/catalogNumber"), recordNumber("http://rs.tdwg.org/dwc/terms/recordNumber"), recordedBy(
    "http://rs.tdwg.org/dwc/terms/recordedBy"), individualCount("http://rs.tdwg.org/dwc/terms/individualCount"), organismQuantity(
    "http://rs.tdwg.org/dwc/terms/organismQuantity"), organismQuantityType(
    "http://rs.tdwg.org/dwc/terms/organismQuantityType"), sex("http://rs.tdwg.org/dwc/terms/sex"), lifeStage(
    "http://rs.tdwg.org/dwc/terms/lifeStage"), reproductiveCondition(
    "http://rs.tdwg.org/dwc/terms/reproductiveCondition"), behavior("http://rs.tdwg.org/dwc/terms/behavior"), establishmentMeans(
    "http://rs.tdwg.org/dwc/terms/establishmentMeans"), occurrenceStatus("http://rs.tdwg.org/dwc/terms/occurrenceStatus"), preparations(
    "http://rs.tdwg.org/dwc/terms/preparations"), disposition("http://rs.tdwg.org/dwc/terms/disposition"), associatedMedia(
    "http://rs.tdwg.org/dwc/terms/associatedMedia"), associatedReferences(
    "http://rs.tdwg.org/dwc/terms/associatedReferences"), associatedSequences(
    "http://rs.tdwg.org/dwc/terms/associatedSequences"), associatedTaxa("http://rs.tdwg.org/dwc/terms/associatedTaxa"), otherCatalogNumbers(
    "http://rs.tdwg.org/dwc/terms/otherCatalogNumbers"), occurrenceRemarks(
    "http://rs.tdwg.org/dwc/terms/occurrenceRemarks"), organismID("http://rs.tdwg.org/dwc/terms/organismID"), organismName(
    "http://rs.tdwg.org/dwc/terms/organismName"), organismScope("http://rs.tdwg.org/dwc/terms/organismScope"), associatedOccurrences(
    "http://rs.tdwg.org/dwc/terms/associatedOccurrences"), associatedOrganisms(
    "http://rs.tdwg.org/dwc/terms/associatedOrganisms"), previousIdentifications(
    "http://rs.tdwg.org/dwc/terms/previousIdentifications"), organismRemarks(
    "http://rs.tdwg.org/dwc/terms/organismRemarks"), materialSampleID("http://rs.tdwg.org/dwc/terms/materialSampleID"), eventID(
    "http://rs.tdwg.org/dwc/terms/eventID"), parentEventID("http://rs.tdwg.org/dwc/terms/parentEventID"), fieldNumber(
    "http://rs.tdwg.org/dwc/terms/fieldNumber"), eventDate("http://rs.tdwg.org/dwc/terms/eventDate"), eventTime(
    "http://rs.tdwg.org/dwc/terms/eventTime"), startDayOfYear("http://rs.tdwg.org/dwc/terms/startDayOfYear"), endDayOfYear(
    "http://rs.tdwg.org/dwc/terms/endDayOfYear"), year("http://rs.tdwg.org/dwc/terms/year"), month(
    "http://rs.tdwg.org/dwc/terms/month"), day("http://rs.tdwg.org/dwc/terms/day"), verbatimEventDate(
    "http://rs.tdwg.org/dwc/terms/verbatimEventDate"), habitat("http://rs.tdwg.org/dwc/terms/habitat"), samplingProtocol(
    "http://rs.tdwg.org/dwc/terms/samplingProtocol"), samplingEffort("http://rs.tdwg.org/dwc/terms/samplingEffort"), sampleSizeValue(
    "http://rs.tdwg.org/dwc/terms/sampleSizeValue"), sampleSizeUnit("http://rs.tdwg.org/dwc/terms/sampleSizeUnit"), fieldNotes(
    "http://rs.tdwg.org/dwc/terms/fieldNotes"), eventRemarks("http://rs.tdwg.org/dwc/terms/eventRemarks"), locationID(
    "http://rs.tdwg.org/dwc/terms/locationID"), higherGeographyID("http://rs.tdwg.org/dwc/terms/higherGeographyID"), higherGeography(
    "http://rs.tdwg.org/dwc/terms/higherGeography"), continent("http://rs.tdwg.org/dwc/terms/continent"), waterBody(
    "http://rs.tdwg.org/dwc/terms/waterBody"), islandGroup("http://rs.tdwg.org/dwc/terms/islandGroup"), island(
    "http://rs.tdwg.org/dwc/terms/island"), country("http://rs.tdwg.org/dwc/terms/country"), countryCode(
    "http://rs.tdwg.org/dwc/terms/countryCode"), stateProvince("http://rs.tdwg.org/dwc/terms/stateProvince"), county(
    "http://rs.tdwg.org/dwc/terms/county"), municipality("http://rs.tdwg.org/dwc/terms/municipality"), locality(
    "http://rs.tdwg.org/dwc/terms/locality"), verbatimLocality("http://rs.tdwg.org/dwc/terms/verbatimLocality"), minimumElevationInMeters(
    "http://rs.tdwg.org/dwc/terms/minimumElevationInMeters"), maximumElevationInMeters(
    "http://rs.tdwg.org/dwc/terms/maximumElevationInMeters"), verbatimElevation(
    "http://rs.tdwg.org/dwc/terms/verbatimElevation"), minimumDepthInMeters(
    "http://rs.tdwg.org/dwc/terms/minimumDepthInMeters"), maximumDepthInMeters(
    "http://rs.tdwg.org/dwc/terms/maximumDepthInMeters"), verbatimDepth("http://rs.tdwg.org/dwc/terms/verbatimDepth"), minimumDistanceAboveSurfaceInMeters(
    "http://rs.tdwg.org/dwc/terms/minimumDistanceAboveSurfaceInMeters"), maximumDistanceAboveSurfaceInMeters(
    "http://rs.tdwg.org/dwc/terms/maximumDistanceAboveSurfaceInMeters"), locationAccordingTo(
    "http://rs.tdwg.org/dwc/terms/locationAccordingTo"), locationRemarks("http://rs.tdwg.org/dwc/terms/locationRemarks"), decimalLatitude(
    "http://rs.tdwg.org/dwc/terms/decimalLatitude"), decimalLongitude("http://rs.tdwg.org/dwc/terms/decimalLongitude"), geodeticDatum(
    "http://rs.tdwg.org/dwc/terms/geodeticDatum"), coordinateUncertaintyInMeters(
    "http://rs.tdwg.org/dwc/terms/coordinateUncertaintyInMeters"), coordinatePrecision(
    "http://rs.tdwg.org/dwc/terms/coordinatePrecision"), pointRadiusSpatialFit(
    "http://rs.tdwg.org/dwc/terms/pointRadiusSpatialFit"), verbatimCoordinates(
    "http://rs.tdwg.org/dwc/terms/verbatimCoordinates"), verbatimLatitude(
    "http://rs.tdwg.org/dwc/terms/verbatimLatitude"), verbatimLongitude("http://rs.tdwg.org/dwc/terms/verbatimLongitude"), verbatimCoordinateSystem(
    "http://rs.tdwg.org/dwc/terms/verbatimCoordinateSystem"), verbatimSRS("http://rs.tdwg.org/dwc/terms/verbatimSRS"), footprintWKT(
    "http://rs.tdwg.org/dwc/terms/footprintWKT"), footprintSRS("http://rs.tdwg.org/dwc/terms/footprintSRS"), footprintSpatialFit(
    "http://rs.tdwg.org/dwc/terms/footprintSpatialFit"), georeferencedBy("http://rs.tdwg.org/dwc/terms/georeferencedBy"), georeferencedDate(
    "http://rs.tdwg.org/dwc/terms/georeferencedDate"), georeferenceProtocol(
    "http://rs.tdwg.org/dwc/terms/georeferenceProtocol"), georeferenceSources(
    "http://rs.tdwg.org/dwc/terms/georeferenceSources"), georeferenceVerificationStatus(
    "http://rs.tdwg.org/dwc/terms/georeferenceVerificationStatus"), georeferenceRemarks(
    "http://rs.tdwg.org/dwc/terms/georeferenceRemarks"), geologicalContextID(
    "http://rs.tdwg.org/dwc/terms/geologicalContextID"), earliestEonOrLowestEonothem(
    "http://rs.tdwg.org/dwc/terms/earliestEonOrLowestEonothem"), latestEonOrHighestEonothem(
    "http://rs.tdwg.org/dwc/terms/latestEonOrHighestEonothem"), earliestEraOrLowestErathem(
    "http://rs.tdwg.org/dwc/terms/earliestEraOrLowestErathem"), latestEraOrHighestErathem(
    "http://rs.tdwg.org/dwc/terms/latestEraOrHighestErathem"), earliestPeriodOrLowestSystem(
    "http://rs.tdwg.org/dwc/terms/earliestPeriodOrLowestSystem"), latestPeriodOrHighestSystem(
    "http://rs.tdwg.org/dwc/terms/latestPeriodOrHighestSystem"), earliestEpochOrLowestSeries(
    "http://rs.tdwg.org/dwc/terms/earliestEpochOrLowestSeries"), latestEpochOrHighestSeries(
    "http://rs.tdwg.org/dwc/terms/latestEpochOrHighestSeries"), earliestAgeOrLowestStage(
    "http://rs.tdwg.org/dwc/terms/earliestAgeOrLowestStage"), latestAgeOrHighestStage(
    "http://rs.tdwg.org/dwc/terms/latestAgeOrHighestStage"), lowestBiostratigraphicZone(
    "http://rs.tdwg.org/dwc/terms/lowestBiostratigraphicZone"), highestBiostratigraphicZone(
    "http://rs.tdwg.org/dwc/terms/highestBiostratigraphicZone"), lithostratigraphicTerms(
    "http://rs.tdwg.org/dwc/terms/lithostratigraphicTerms"), group("http://rs.tdwg.org/dwc/terms/group"), formation(
    "http://rs.tdwg.org/dwc/terms/formation"), member("http://rs.tdwg.org/dwc/terms/member"), bed(
    "http://rs.tdwg.org/dwc/terms/bed"), identificationID("http://rs.tdwg.org/dwc/terms/identificationID"), identificationQualifier(
    "http://rs.tdwg.org/dwc/terms/identificationQualifier"), typeStatus("http://rs.tdwg.org/dwc/terms/typeStatus"), identifiedBy(
    "http://rs.tdwg.org/dwc/terms/identifiedBy"), dateIdentified("http://rs.tdwg.org/dwc/terms/dateIdentified"), identificationReferences(
    "http://rs.tdwg.org/dwc/terms/identificationReferences"), identificationVerificationStatus(
    "http://rs.tdwg.org/dwc/terms/identificationVerificationStatus"), identificationRemarks(
    "http://rs.tdwg.org/dwc/terms/identificationRemarks"), taxonID("http://rs.tdwg.org/dwc/terms/taxonID"), scientificNameID(
    "http://rs.tdwg.org/dwc/terms/scientificNameID"), acceptedNameUsageID(
    "http://rs.tdwg.org/dwc/terms/acceptedNameUsageID"), parentNameUsageID(
    "http://rs.tdwg.org/dwc/terms/parentNameUsageID"), originalNameUsageID(
    "http://rs.tdwg.org/dwc/terms/originalNameUsageID"), nameAccordingToID(
    "http://rs.tdwg.org/dwc/terms/nameAccordingToID"), namePublishedInID(
    "http://rs.tdwg.org/dwc/terms/namePublishedInID"), taxonConceptID("http://rs.tdwg.org/dwc/terms/taxonConceptID"), scientificName(
    "http://rs.tdwg.org/dwc/terms/scientificName"), acceptedNameUsage("http://rs.tdwg.org/dwc/terms/acceptedNameUsage"), parentNameUsage(
    "http://rs.tdwg.org/dwc/terms/parentNameUsage"), originalNameUsage("http://rs.tdwg.org/dwc/terms/originalNameUsage"), nameAccordingTo(
    "http://rs.tdwg.org/dwc/terms/nameAccordingTo"), namePublishedIn("http://rs.tdwg.org/dwc/terms/namePublishedIn"), namePublishedInYear(
    "http://rs.tdwg.org/dwc/terms/namePublishedInYear"), higherClassification(
    "http://rs.tdwg.org/dwc/terms/higherClassification"), kingdom("http://rs.tdwg.org/dwc/terms/kingdom"), phylum(
    "http://rs.tdwg.org/dwc/terms/phylum"), clazz("http://rs.tdwg.org/dwc/terms/class"), order(
    "http://rs.tdwg.org/dwc/terms/order"), family("http://rs.tdwg.org/dwc/terms/family"), genus(
    "http://rs.tdwg.org/dwc/terms/genus"), subgenus("http://rs.tdwg.org/dwc/terms/subgenus"), specificEpithet(
    "http://rs.tdwg.org/dwc/terms/specificEpithet"), infraspecificEpithet(
    "http://rs.tdwg.org/dwc/terms/infraspecificEpithet"), taxonRank("http://rs.tdwg.org/dwc/terms/taxonRank"), verbatimTaxonRank(
    "http://rs.tdwg.org/dwc/terms/verbatimTaxonRank"), scientificNameAuthorship(
    "http://rs.tdwg.org/dwc/terms/scientificNameAuthorship"), vernacularName(
    "http://rs.tdwg.org/dwc/terms/vernacularName"), nomenclaturalCode("http://rs.tdwg.org/dwc/terms/nomenclaturalCode"), taxonomicStatus(
    "http://rs.tdwg.org/dwc/terms/taxonomicStatus"), nomenclaturalStatus(
    "http://rs.tdwg.org/dwc/terms/nomenclaturalStatus"), taxonRemarks("http://rs.tdwg.org/dwc/terms/taxonRemarks"), measurementID(
    "http://rs.tdwg.org/dwc/terms/measurementID"), measurementType("http://rs.tdwg.org/dwc/terms/measurementType"), measurementValue(
    "http://rs.tdwg.org/dwc/terms/measurementValue"), measurementAccuracy(
    "http://rs.tdwg.org/dwc/terms/measurementAccuracy"), measurementUnit("http://rs.tdwg.org/dwc/terms/measurementUnit"), measurementDeterminedBy(
    "http://rs.tdwg.org/dwc/terms/measurementDeterminedBy"), measurementDeterminedDate(
    "http://rs.tdwg.org/dwc/terms/measurementDeterminedDate"), measurementMethod(
    "http://rs.tdwg.org/dwc/terms/measurementMethod"), measurementRemarks(
    "http://rs.tdwg.org/dwc/terms/measurementRemarks"), resourceRelationshipID(
    "http://rs.tdwg.org/dwc/terms/resourceRelationshipID"), resourceID("http://rs.tdwg.org/dwc/terms/resourceID"), relatedResourceID(
    "http://rs.tdwg.org/dwc/terms/relatedResourceID"), relationshipOfResource(
    "http://rs.tdwg.org/dwc/terms/relationshipOfResource"), relationshipAccordingTo(
    "http://rs.tdwg.org/dwc/terms/relationshipAccordingTo"), relationshipEstablishedDate(
    "http://rs.tdwg.org/dwc/terms/relationshipEstablishedDate"), relationshipRemarks(
    "http://rs.tdwg.org/dwc/terms/relationshipRemarks");

  private String identifier;

  DwCATermIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public String getIdentifier() {
    return this.identifier;
  }

}
