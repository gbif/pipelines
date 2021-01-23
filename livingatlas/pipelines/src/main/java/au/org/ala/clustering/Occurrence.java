package au.org.ala.clustering;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

@Value
@Builder
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class Occurrence {

  String uuid; // replacing gbifId
  String dataResourceUid; // replacing datasetKey
  boolean isSpecimen;

  String basisOfRecord;
  String publishingorgkey;
  String datasetName;
  String publisher;
  String kingdomKey;
  String phylumKey;
  String classKey;
  String orderKey;
  String familyKey;
  String genusKey;
  String speciesKey;
  String acceptedTaxonKey;
  String taxonKey;
  String scientificName;
  String acceptedScientificName;
  String kingdom;
  String phylum;
  String order;
  String family;
  String genus;
  String species;
  String genericName;
  String specificEpithet;
  String taxonRank;
  String typeStatus;
  String preparations;
  String decimalLatitude;
  String decimalLongitude;
  String countryCode;
  Integer year;
  Integer month;
  Integer day;

  // from_unixtime(floor(t1.eventDate/1000)) AS t1_
  Long eventDate;

  String recordNumber;
  String fieldNumber;
  String occurrenceID;
  String otherCatalogNumbers;
  String institutionCode;
  String collectionCode;
  String catalogNumber;
  String recordedBy;
  String recordedByID;
  String ext_multimediaString;
}
