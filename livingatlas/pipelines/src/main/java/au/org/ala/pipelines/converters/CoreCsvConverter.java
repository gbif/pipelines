package au.org.ala.pipelines.converters;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.IndexRecord;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CoreCsvConverter {

  private static final String RAW = "raw_";

  private static final TsvConverter<IndexRecord> CONVERTER =
      TsvConverter.<IndexRecord>create()
          // DWC Terms
          .addKeyTermFn(DwcTerm.occurrenceID, ir -> Optional.of(ir.getId()))
          .addKeyTermFn(DwcTerm.catalogNumber, getString(DwcTerm.catalogNumber))
          .addKeyTermFn(DwcTerm.collectionCode, getString(DwcTerm.collectionCode))
          .addKeyTermFn(DwcTerm.institutionCode, getString(DwcTerm.institutionCode))
          .addKeyTermFn(DwcTerm.recordNumber, getString(DwcTerm.recordNumber))
          .addKeyTermFn(DwcTerm.basisOfRecord, getString(DwcTerm.basisOfRecord), "HumanObservation")
          .addKeyTermFn(DwcTerm.recordedBy, getString(DwcTerm.recordedBy))
          .addKeyTermFn(DwcTerm.occurrenceStatus, getString(DwcTerm.occurrenceStatus))
          .addKeyTermFn(DwcTerm.individualCount, getInt(DwcTerm.individualCount))
          .addKeyTermFn(DwcTerm.scientificName, getString(DwcTerm.scientificName))
          .addKeyTermFn(DwcTerm.taxonConceptID, getString(DwcTerm.taxonConceptID))
          .addKeyTermFn(DwcTerm.taxonRank, getString(DwcTerm.taxonRank))
          .addKeyTermFn(DwcTerm.kingdom, getString(DwcTerm.kingdom))
          .addKeyTermFn(DwcTerm.phylum, getString(DwcTerm.phylum))
          .addKeyTermFn(DwcTerm.class_, getString(DwcTerm.class_))
          .addKeyTermFn(DwcTerm.order, getString(DwcTerm.order))
          .addKeyTermFn(DwcTerm.family, getString(DwcTerm.family))
          .addKeyTermFn(DwcTerm.genus, getString(DwcTerm.genus))
          .addKeyTermFn(DwcTerm.vernacularName, getString(DwcTerm.vernacularName))
          .addKeyTermFn(DwcTerm.decimalLatitude, getDouble(DwcTerm.decimalLatitude))
          .addKeyTermFn(DwcTerm.decimalLongitude, getDouble(DwcTerm.decimalLongitude))
          .addKeyTermFn(DwcTerm.geodeticDatum, getString(DwcTerm.geodeticDatum))
          .addKeyTermFn(
              DwcTerm.coordinateUncertaintyInMeters,
              getDouble(DwcTerm.coordinateUncertaintyInMeters))
          .addKeyTermFn(
              DwcTerm.maximumElevationInMeters, getDouble(DwcTerm.maximumElevationInMeters))
          .addKeyTermFn(
              DwcTerm.minimumElevationInMeters, getDouble(DwcTerm.minimumElevationInMeters))
          .addKeyTermFn(DwcTerm.minimumDepthInMeters, getDouble(DwcTerm.minimumDepthInMeters))
          .addKeyTermFn(DwcTerm.maximumDepthInMeters, getDouble(DwcTerm.maximumDepthInMeters))
          .addKeyTermFn(DwcTerm.country, getString(DwcTerm.country))
          .addKeyTermFn(DwcTerm.stateProvince, getString(DwcTerm.stateProvince))
          .addKeyTermFn(DwcTerm.locality, getString(DwcTerm.locality))
          .addKeyTermFn(DwcTerm.locationRemarks, getString(DwcTerm.locationRemarks))
          .addKeyTermFn(DwcTerm.year, getInt(DwcTerm.year))
          .addKeyTermFn(DwcTerm.month, getInt(DwcTerm.month))
          .addKeyTermFn(DwcTerm.day, getInt(DwcTerm.day))
          .addKeyTermFn(DwcTerm.eventDate, getRawString(DwcTerm.eventDate))
          .addKeyTermFn(DwcTerm.eventID, getString(DwcTerm.eventID))
          .addKeyTermFn(DwcTerm.identifiedBy, getMultivalue(DwcTerm.identifiedBy))
          .addKeyTermFn(DwcTerm.occurrenceRemarks, getString(DwcTerm.occurrenceRemarks))
          .addKeyTermFn(DwcTerm.dataGeneralizations, getString(DwcTerm.dataGeneralizations))
          .addKeyTermFn(DwcTerm.otherCatalogNumbers, getMultivalue(DwcTerm.otherCatalogNumbers))
          .addKeyTermFn(DwcTerm.acceptedNameUsage, getString(DwcTerm.acceptedNameUsage))
          .addKeyTermFn(DwcTerm.acceptedNameUsageID, getString(DwcTerm.acceptedNameUsageID))
          .addKeyTermFn(DwcTerm.associatedOccurrences, getString(DwcTerm.associatedOccurrences))
          .addKeyTermFn(DwcTerm.associatedReferences, getString(DwcTerm.associatedReferences))
          .addKeyTermFn(DwcTerm.associatedSequences, getString(DwcTerm.associatedSequences))
          .addKeyTermFn(DwcTerm.associatedTaxa, getString(DwcTerm.associatedTaxa))
          .addKeyTermFn(DwcTerm.behavior, getString(DwcTerm.behavior))
          .addKeyTermFn(DwcTerm.collectionID, getString(DwcTerm.collectionID))
          .addKeyTermFn(DwcTerm.continent, getString(DwcTerm.continent))
          .addKeyTermFn(DwcTerm.coordinatePrecision, getDouble(DwcTerm.coordinatePrecision))
          .addKeyTermFn(DwcTerm.countryCode, getString(DwcTerm.countryCode))
          .addKeyTermFn(DwcTerm.county, getString(DwcTerm.county))
          .addKeyTermFn(DwcTerm.datasetID, getMultivalue(DwcTerm.datasetID))
          .addKeyTermFn(DwcTerm.datasetName, getMultivalue(DwcTerm.datasetName))
          .addKeyTermFn(DwcTerm.dateIdentified, getString(DwcTerm.dateIdentified))
          .addKeyTermFn(DwcTerm.degreeOfEstablishment, getString(DwcTerm.degreeOfEstablishment))
          .addKeyTermFn(DwcTerm.disposition, getString(DwcTerm.disposition))
          .addKeyTermFn(DwcTerm.dynamicProperties, getString(DwcTerm.dynamicProperties))
          .addKeyTermFn(DwcTerm.endDayOfYear, getInt(DwcTerm.endDayOfYear))
          .addKeyTermFn(DwcTerm.establishmentMeans, getString(DwcTerm.establishmentMeans))
          .addKeyTermFn(DwcTerm.eventRemarks, getString(DwcTerm.eventRemarks))
          .addKeyTermFn(DwcTerm.eventTime, getString(DwcTerm.eventTime))
          .addKeyTermFn(DwcTerm.fieldNotes, getString(DwcTerm.fieldNotes))
          .addKeyTermFn(DwcTerm.fieldNumber, getString(DwcTerm.fieldNumber))
          .addKeyTermFn(DwcTerm.footprintSpatialFit, getString(DwcTerm.footprintSpatialFit))
          .addKeyTermFn(DwcTerm.footprintSRS, getString(DwcTerm.footprintSRS))
          .addKeyTermFn(DwcTerm.footprintWKT, getString(DwcTerm.footprintWKT))
          .addKeyTermFn(DwcTerm.georeferencedBy, getString(DwcTerm.georeferencedBy))
          .addKeyTermFn(DwcTerm.georeferencedDate, getString(DwcTerm.georeferencedDate))
          .addKeyTermFn(DwcTerm.georeferenceProtocol, getString(DwcTerm.georeferenceProtocol))
          .addKeyTermFn(DwcTerm.georeferenceRemarks, getString(DwcTerm.georeferenceRemarks))
          .addKeyTermFn(DwcTerm.georeferenceSources, getString(DwcTerm.georeferenceSources))
          .addKeyTermFn(
              DwcTerm.georeferenceVerificationStatus,
              getString(DwcTerm.georeferenceVerificationStatus))
          .addKeyTermFn(DwcTerm.habitat, getString(DwcTerm.habitat))
          .addKeyTermFn(DwcTerm.higherClassification, getString(DwcTerm.higherClassification))
          .addKeyTermFn(DwcTerm.higherGeography, getString(DwcTerm.higherGeography))
          .addKeyTermFn(DwcTerm.higherGeographyID, getString(DwcTerm.higherGeographyID))
          .addKeyTermFn(DwcTerm.identificationID, getString(DwcTerm.identificationID))
          .addKeyTermFn(DwcTerm.identificationQualifier, getString(DwcTerm.identificationQualifier))
          .addKeyTermFn(
              DwcTerm.identificationReferences, getString(DwcTerm.identificationReferences))
          .addKeyTermFn(DwcTerm.identificationRemarks, getString(DwcTerm.identificationRemarks))
          .addKeyTermFn(
              DwcTerm.identificationVerificationStatus,
              getString(DwcTerm.identificationVerificationStatus))
          .addKeyTermFn(DwcTerm.informationWithheld, getString(DwcTerm.informationWithheld))
          .addKeyTermFn(DwcTerm.infraspecificEpithet, getString(DwcTerm.infraspecificEpithet))
          .addKeyTermFn(DwcTerm.institutionID, getString(DwcTerm.institutionID))
          .addKeyTermFn(DwcTerm.island, getString(DwcTerm.island))
          .addKeyTermFn(DwcTerm.islandGroup, getString(DwcTerm.islandGroup))
          .addKeyTermFn(DwcTerm.lifeStage, getString(DwcTerm.lifeStage))
          .addKeyTermFn(DwcTerm.locationAccordingTo, getString(DwcTerm.locationAccordingTo))
          .addKeyTermFn(DwcTerm.locationID, getString(DwcTerm.locationID))
          .addKeyTermFn(
              DwcTerm.maximumDistanceAboveSurfaceInMeters,
              getDouble(DwcTerm.maximumDistanceAboveSurfaceInMeters))
          .addKeyTermFn(DwcTerm.measurementAccuracy, getString(DwcTerm.measurementAccuracy))
          .addKeyTermFn(DwcTerm.measurementDeterminedBy, getString(DwcTerm.measurementDeterminedBy))
          .addKeyTermFn(
              DwcTerm.measurementDeterminedDate, getString(DwcTerm.measurementDeterminedDate))
          .addKeyTermFn(DwcTerm.measurementID, getString(DwcTerm.measurementID))
          .addKeyTermFn(DwcTerm.measurementMethod, getString(DwcTerm.measurementMethod))
          .addKeyTermFn(DwcTerm.measurementRemarks, getString(DwcTerm.measurementRemarks))
          .addKeyTermFn(DwcTerm.measurementType, getString(DwcTerm.measurementType))
          .addKeyTermFn(DwcTerm.measurementUnit, getString(DwcTerm.measurementUnit))
          .addKeyTermFn(DwcTerm.measurementValue, getString(DwcTerm.measurementValue))
          .addKeyTermFn(DwcTerm.municipality, getString(DwcTerm.municipality))
          .addKeyTermFn(DwcTerm.nameAccordingTo, getString(DwcTerm.nameAccordingTo))
          .addKeyTermFn(DwcTerm.nameAccordingToID, getString(DwcTerm.nameAccordingToID))
          .addKeyTermFn(DwcTerm.namePublishedIn, getString(DwcTerm.namePublishedIn))
          .addKeyTermFn(DwcTerm.namePublishedInID, getString(DwcTerm.namePublishedInID))
          .addKeyTermFn(DwcTerm.namePublishedInYear, getString(DwcTerm.namePublishedInYear))
          .addKeyTermFn(DwcTerm.nomenclaturalCode, getString(DwcTerm.nomenclaturalCode))
          .addKeyTermFn(DwcTerm.nomenclaturalStatus, getString(DwcTerm.nomenclaturalStatus))
          .addKeyTermFn(DwcTerm.organismID, getString(DwcTerm.organismID))
          .addKeyTermFn(DwcTerm.organismQuantity, getDouble(DwcTerm.organismQuantity))
          .addKeyTermFn(DwcTerm.organismQuantityType, getString(DwcTerm.organismQuantityType))
          .addKeyTermFn(DwcTerm.originalNameUsage, getString(DwcTerm.originalNameUsage))
          .addKeyTermFn(DwcTerm.originalNameUsageID, getString(DwcTerm.originalNameUsageID))
          .addKeyTermFn(DwcTerm.ownerInstitutionCode, getString(DwcTerm.ownerInstitutionCode))
          .addKeyTermFn(DwcTerm.parentNameUsage, getString(DwcTerm.parentNameUsage))
          .addKeyTermFn(DwcTerm.parentNameUsageID, getString(DwcTerm.parentNameUsageID))
          .addKeyTermFn(DwcTerm.pointRadiusSpatialFit, getString(DwcTerm.pointRadiusSpatialFit))
          .addKeyTermFn(DwcTerm.preparations, getMultivalue(DwcTerm.preparations))
          .addKeyTermFn(DwcTerm.previousIdentifications, getString(DwcTerm.previousIdentifications))
          .addKeyTermFn(DwcTerm.relatedResourceID, getString(DwcTerm.relatedResourceID))
          .addKeyTermFn(DwcTerm.relationshipAccordingTo, getString(DwcTerm.relationshipAccordingTo))
          .addKeyTermFn(
              DwcTerm.minimumDistanceAboveSurfaceInMeters,
              getDouble(DwcTerm.minimumDistanceAboveSurfaceInMeters))
          .addKeyTermFn(
              DwcTerm.relationshipEstablishedDate, getString(DwcTerm.relationshipEstablishedDate))
          .addKeyTermFn(DwcTerm.relationshipOfResource, getString(DwcTerm.relationshipOfResource))
          .addKeyTermFn(DwcTerm.relationshipRemarks, getString(DwcTerm.relationshipRemarks))
          .addKeyTermFn(DwcTerm.reproductiveCondition, getString(DwcTerm.reproductiveCondition))
          .addKeyTermFn(DwcTerm.resourceID, getString(DwcTerm.resourceID))
          .addKeyTermFn(DwcTerm.resourceRelationshipID, getString(DwcTerm.resourceRelationshipID))
          .addKeyTermFn(DwcTerm.samplingEffort, getString(DwcTerm.samplingEffort))
          .addKeyTermFn(DwcTerm.samplingProtocol, getMultivalue(DwcTerm.samplingProtocol))
          .addKeyTermFn(
              DwcTerm.scientificNameAuthorship, getString(DwcTerm.scientificNameAuthorship))
          .addKeyTermFn(DwcTerm.scientificNameID, getString(DwcTerm.scientificNameID))
          .addKeyTermFn(DwcTerm.sex, getString(DwcTerm.sex))
          .addKeyTermFn(DwcTerm.specificEpithet, getString(DwcTerm.specificEpithet))
          .addKeyTermFn(DwcTerm.startDayOfYear, getInt(DwcTerm.startDayOfYear))
          .addKeyTermFn(DwcTerm.subgenus, getString(DwcTerm.subgenus))
          .addKeyTermFn(DwcTerm.taxonID, getString(DwcTerm.taxonID))
          .addKeyTermFn(DwcTerm.taxonomicStatus, getString(DwcTerm.taxonomicStatus))
          .addKeyTermFn(DwcTerm.taxonRemarks, getString(DwcTerm.taxonRemarks))
          .addKeyTermFn(DwcTerm.typeStatus, getMultivalue(DwcTerm.typeStatus))
          .addKeyTermFn(DwcTerm.verbatimCoordinates, getString(DwcTerm.verbatimCoordinates))
          .addKeyTermFn(
              DwcTerm.verbatimCoordinateSystem, getString(DwcTerm.verbatimCoordinateSystem))
          .addKeyTermFn(DwcTerm.verbatimDepth, getString(DwcTerm.verbatimDepth))
          .addKeyTermFn(DwcTerm.verbatimElevation, getString(DwcTerm.verbatimElevation))
          .addKeyTermFn(DwcTerm.verbatimEventDate, getString(DwcTerm.verbatimEventDate))
          .addKeyTermFn(DwcTerm.verbatimLatitude, getString(DwcTerm.verbatimLatitude))
          .addKeyTermFn(DwcTerm.verbatimLocality, getString(DwcTerm.verbatimLocality))
          .addKeyTermFn(DwcTerm.verbatimLongitude, getString(DwcTerm.verbatimLongitude))
          .addKeyTermFn(DwcTerm.verbatimSRS, getString(DwcTerm.verbatimSRS))
          .addKeyTermFn(DwcTerm.verbatimTaxonRank, getString(DwcTerm.verbatimTaxonRank))
          .addKeyTermFn(DwcTerm.waterBody, getString(DwcTerm.waterBody))
          .addKeyTermFn(
              "http://rs.tdwg.org/dwc/terms/occurrenceAttributes",
              getString("occurrenceAttributes"))
          // DC Terms
          .addKeyTermFn(DcTerm.references, getString(DcTerm.references))
          .addKeyTermFn(DcTerm.accessRights, getString(DcTerm.accessRights))
          .addKeyTermFn(DcTerm.bibliographicCitation, getString(DcTerm.bibliographicCitation))
          .addKeyTermFn(DcTerm.language, getString(DcTerm.language))
          .addKeyTermFn(DcTerm.license, getString(DcTerm.license))
          .addKeyTermFn(DcTerm.modified, getString(DcTerm.modified))
          .addKeyTermFn(DcTerm.rights, getString(DcTerm.rights))
          .addKeyTermFn(DcTerm.rightsHolder, getString(DcTerm.rightsHolder))
          .addKeyTermFn(DcTerm.source, getString(DcTerm.source))
          .addKeyTermFn(DcTerm.type, getString(DcTerm.type))
          // ALA Terms
          .addKeyTermFn("http://rs.ala.org.au/terms/1.0/photographer", getString("photographer"))
          .addKeyTermFn("http://rs.ala.org.au/terms/1.0/northing", getString("northing"))
          .addKeyTermFn("http://rs.ala.org.au/terms/1.0/easting", getString("easting"))
          .addKeyTermFn("http://rs.ala.org.au/terms/1.0/species", getString("species"))
          .addKeyTermFn("http://rs.ala.org.au/terms/1.0/subfamily", getString("subfamily"))
          .addKeyTermFn("http://rs.ala.org.au/terms/1.0/subspecies", getString("subspecies"))
          .addKeyTermFn("http://rs.ala.org.au/terms/1.0/superfamily", getString("superfamily"))
          .addKeyTermFn("http://rs.ala.org.au/terms/1.0/zone", getString("zone"))
          // ABCD Terms
          .addKeyTermFn(
              "http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifier",
              getString("abcdIdentificationQualifier"))
          .addKeyTermFn(
              "http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifierInsertionPoint",
              getString("abcdIdentificationQualifierInsertionPoint"))
          .addKeyTermFn("http://rs.tdwg.org/abcd/terms/abcdTypeStatus", getString("abcdTypeStatus"))
          .addKeyTermFn("http://rs.tdwg.org/abcd/terms/typifiedName", getString("typifiedName"))
          // HISPID Terms
          .addKeyTermFn(
              "http://hiscom.chah.org.au/hispid/terms/secondaryCollectors",
              getString("secondaryCollectors"))
          .addKeyTermFn(
              "http://hiscom.chah.org.au/hispid/terms/identifierRole", getString("identifierRole"))
          // GGBN Terms
          .addKeyTermFn("http://data.ggbn.org/schemas/ggbn/terms/loanDate", getString("loanDate"))
          .addKeyTermFn(
              "http://data.ggbn.org/schemas/ggbn/terms/loanDestination",
              getString("loanDestination"))
          .addKeyTermFn(
              "http://data.ggbn.org/schemas/ggbn/terms/loanIdentifier", getString("loanIdentifier"))
          .addKeyTermFn(
              "http://rs.tdwg.org/dwc/terms/locationAttributes", getString("locationAttributes"))
          .addKeyTermFn(
              "http://rs.tdwg.org/dwc/terms/eventAttributes", getString("eventAttributes"))
          // Other Terms
          .addKeyTermFn("taxonRankID", getInt("taxonRankID"))
          // GBIF Terms
          .addKeyTermFn(DwcTerm.recordedByID, getMultivalue(DwcTerm.recordedByID));

  public static String convert(IndexRecord indexRecord) {
    return CONVERTER.converter(indexRecord);
  }

  public static List<String> getTerms() {
    return CONVERTER.getTerms();
  }

  private static Function<IndexRecord, Optional<String>> getRawString(Term term) {
    return getString(RAW + term.simpleName());
  }

  private static Function<IndexRecord, Optional<String>> getString(Term term) {
    return getString(term.simpleName());
  }

  private static Function<IndexRecord, Optional<String>> getString(String key) {
    return ir -> Optional.ofNullable(ir.getStrings().get(key));
  }

  private static Function<IndexRecord, Optional<String>> getDouble(Term term) {
    return ir -> {
      Double d = ir.getDoubles().get(term.simpleName());
      String result = null;
      if (d != null) {
        result = d.toString();
      }
      return Optional.ofNullable(result);
    };
  }

  private static Function<IndexRecord, Optional<String>> getInt(String term) {
    return ir -> {
      Integer d = ir.getInts().get(term);
      String result = null;
      if (d != null) {
        result = d.toString();
      }
      return Optional.ofNullable(result);
    };
  }

  private static Function<IndexRecord, Optional<String>> getInt(Term term) {
    return getInt(term.simpleName());
  }

  private static Function<IndexRecord, Optional<String>> getMultivalue(Term term) {
    return ir ->
        Optional.ofNullable(ir.getMultiValues().get(term.simpleName()))
            .map(x -> String.join("|", x));
  }
}
