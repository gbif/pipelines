package au.org.ala.clustering;

import java.util.List;

/** Builder for {@link HashKeyOccurrence} */
public final class HashKeyOccurrenceBuilder {
  String hashKey = null;
  String id = null;
  String datasetKey = null;
  String speciesKey = null;
  String taxonKey = null;
  String basisOfRecord = null;
  Double decimalLatitude = null;
  Double decimalLongitude = null;
  Integer year = null;
  Integer month = null;
  Integer day = null;
  String eventDate = null;
  String scientificName = null;
  String countryCode = null;
  List<String> typeStatus = null;
  String occurrenceID = null;
  List<String> recordedBy = null;
  String fieldNumber = null;
  String recordNumber = null;
  String catalogNumber = null;
  List<String> otherCatalogNumbers = null;

  private HashKeyOccurrenceBuilder() {}

  public static HashKeyOccurrenceBuilder aHashKeyOccurrence() {
    return new HashKeyOccurrenceBuilder();
  }

  public HashKeyOccurrenceBuilder withHashKey(String hashKey) {
    this.hashKey = hashKey;
    return this;
  }

  public HashKeyOccurrenceBuilder withId(String id) {
    this.id = id;
    return this;
  }

  public HashKeyOccurrenceBuilder withDatasetKey(String datasetKey) {
    this.datasetKey = datasetKey;
    return this;
  }

  public HashKeyOccurrenceBuilder withSpeciesKey(String speciesKey) {
    this.speciesKey = speciesKey;
    return this;
  }

  public HashKeyOccurrenceBuilder withTaxonKey(String taxonKey) {
    this.taxonKey = taxonKey;
    return this;
  }

  public HashKeyOccurrenceBuilder withBasisOfRecord(String basisOfRecord) {
    this.basisOfRecord = basisOfRecord;
    return this;
  }

  public HashKeyOccurrenceBuilder withDecimalLatitude(Double decimalLatitude) {
    this.decimalLatitude = decimalLatitude;
    return this;
  }

  public HashKeyOccurrenceBuilder withDecimalLongitude(Double decimalLongitude) {
    this.decimalLongitude = decimalLongitude;
    return this;
  }

  public HashKeyOccurrenceBuilder withYear(Integer year) {
    this.year = year;
    return this;
  }

  public HashKeyOccurrenceBuilder withMonth(Integer month) {
    this.month = month;
    return this;
  }

  public HashKeyOccurrenceBuilder withDay(Integer day) {
    this.day = day;
    return this;
  }

  public HashKeyOccurrenceBuilder withEventDate(String eventDate) {
    this.eventDate = eventDate;
    return this;
  }

  public HashKeyOccurrenceBuilder withScientificName(String scientificName) {
    this.scientificName = scientificName;
    return this;
  }

  public HashKeyOccurrenceBuilder withCountryCode(String countryCode) {
    this.countryCode = countryCode;
    return this;
  }

  public HashKeyOccurrenceBuilder withTypeStatus(List<String> typeStatus) {
    this.typeStatus = typeStatus;
    return this;
  }

  public HashKeyOccurrenceBuilder withOccurrenceID(String occurrenceID) {
    this.occurrenceID = occurrenceID;
    return this;
  }

  public HashKeyOccurrenceBuilder withRecordedBy(List<String> recordedBy) {
    this.recordedBy = recordedBy;
    return this;
  }

  public HashKeyOccurrenceBuilder withFieldNumber(String fieldNumber) {
    this.fieldNumber = fieldNumber;
    return this;
  }

  public HashKeyOccurrenceBuilder withRecordNumber(String recordNumber) {
    this.recordNumber = recordNumber;
    return this;
  }

  public HashKeyOccurrenceBuilder withCatalogNumber(String catalogNumber) {
    this.catalogNumber = catalogNumber;
    return this;
  }

  public HashKeyOccurrenceBuilder withOtherCatalogNumbers(List<String> otherCatalogNumbers) {
    this.otherCatalogNumbers = otherCatalogNumbers;
    return this;
  }

  public HashKeyOccurrence build() {
    HashKeyOccurrence hashKeyOccurrence = new HashKeyOccurrence();
    hashKeyOccurrence.setHashKey(hashKey);
    hashKeyOccurrence.setId(id);
    hashKeyOccurrence.setDatasetKey(datasetKey);
    hashKeyOccurrence.setSpeciesKey(speciesKey);
    hashKeyOccurrence.setTaxonKey(taxonKey);
    hashKeyOccurrence.setBasisOfRecord(basisOfRecord);
    hashKeyOccurrence.setDecimalLatitude(decimalLatitude);
    hashKeyOccurrence.setDecimalLongitude(decimalLongitude);
    hashKeyOccurrence.setYear(year);
    hashKeyOccurrence.setMonth(month);
    hashKeyOccurrence.setDay(day);
    hashKeyOccurrence.setEventDate(eventDate);
    hashKeyOccurrence.setScientificName(scientificName);
    hashKeyOccurrence.setCountryCode(countryCode);
    hashKeyOccurrence.setTypeStatus(typeStatus);
    hashKeyOccurrence.setOccurrenceID(occurrenceID);
    hashKeyOccurrence.setRecordedBy(recordedBy);
    hashKeyOccurrence.setFieldNumber(fieldNumber);
    hashKeyOccurrence.setRecordNumber(recordNumber);
    hashKeyOccurrence.setCatalogNumber(catalogNumber);
    hashKeyOccurrence.setOtherCatalogNumbers(otherCatalogNumbers);
    return hashKeyOccurrence;
  }
}
