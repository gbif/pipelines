package au.org.ala.clustering;

import java.util.List;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;

/** An occurrence with hashkey. The hashkey is used to group related occurrences. */
@DefaultCoder(SchemaCoder.class)
@DefaultSchema(JavaBeanSchema.class)
public class HashKeyOccurrence implements OccurrenceFeatures {

  @Nullable String hashKey = null;
  @Nullable String id = null;
  @Nullable String datasetKey = null;
  @Nullable String speciesKey = null;
  @Nullable String taxonKey = null;
  @Nullable String basisOfRecord = null;
  @Nullable Double decimalLatitude = null;
  @Nullable Double decimalLongitude = null;
  @Nullable Integer year = null;
  @Nullable Integer month = null;
  @Nullable Integer day = null;
  @Nullable String eventDate = null;
  @Nullable String scientificName = null;
  @Nullable String countryCode = null;
  @Nullable List<String> typeStatus = null;
  @Nullable String occurrenceID = null;
  @Nullable List<String> recordedBy = null;
  @Nullable String fieldNumber = null;
  @Nullable String recordNumber = null;
  @Nullable String catalogNumber = null;
  @Nullable List<String> otherCatalogNumbers = null;
  @Nullable String institutionCode = null;
  @Nullable String collectionCode = null;

  public @Nullable String getHashKey() {
    return hashKey;
  }

  public void setHashKey(@Nullable String hashKey) {
    this.hashKey = hashKey;
  }

  @Override
  public @Nullable String getId() {
    return id;
  }

  public void setId(@Nullable String id) {
    this.id = id;
  }

  @Override
  public @Nullable String getDatasetKey() {
    return datasetKey;
  }

  public void setDatasetKey(@Nullable String datasetKey) {
    this.datasetKey = datasetKey;
  }

  @Override
  public @Nullable String getSpeciesKey() {
    return speciesKey;
  }

  public void setSpeciesKey(@Nullable String speciesKey) {
    this.speciesKey = speciesKey;
  }

  @Override
  public @Nullable String getTaxonKey() {
    return taxonKey;
  }

  public void setTaxonKey(@Nullable String taxonKey) {
    this.taxonKey = taxonKey;
  }

  @Override
  public @Nullable String getBasisOfRecord() {
    return basisOfRecord;
  }

  public void setBasisOfRecord(@Nullable String basisOfRecord) {
    this.basisOfRecord = basisOfRecord;
  }

  @Override
  public @Nullable Double getDecimalLatitude() {
    return decimalLatitude;
  }

  public void setDecimalLatitude(@Nullable Double decimalLatitude) {
    this.decimalLatitude = decimalLatitude;
  }

  @Override
  public @Nullable Double getDecimalLongitude() {
    return decimalLongitude;
  }

  public void setDecimalLongitude(@Nullable Double decimalLongitude) {
    this.decimalLongitude = decimalLongitude;
  }

  @Override
  public @Nullable Integer getYear() {
    return year;
  }

  public void setYear(@Nullable Integer year) {
    this.year = year;
  }

  @Override
  public @Nullable Integer getMonth() {
    return month;
  }

  public void setMonth(@Nullable Integer month) {
    this.month = month;
  }

  @Override
  public @Nullable Integer getDay() {
    return day;
  }

  public void setDay(@Nullable Integer day) {
    this.day = day;
  }

  @Override
  public @Nullable String getEventDate() {
    return eventDate;
  }

  public void setEventDate(@Nullable String eventDate) {
    this.eventDate = eventDate;
  }

  @Override
  public @Nullable String getScientificName() {
    return scientificName;
  }

  public void setScientificName(@Nullable String scientificName) {
    this.scientificName = scientificName;
  }

  @Override
  public @Nullable String getCountryCode() {
    return countryCode;
  }

  public void setCountryCode(@Nullable String countryCode) {
    this.countryCode = countryCode;
  }

  @Override
  public @Nullable List<String> getTypeStatus() {
    return typeStatus;
  }

  public void setTypeStatus(@Nullable List<String> typeStatus) {
    this.typeStatus = typeStatus;
  }

  @Override
  public @Nullable String getOccurrenceID() {
    return occurrenceID;
  }

  public void setOccurrenceID(@Nullable String occurrenceID) {
    this.occurrenceID = occurrenceID;
  }

  @Override
  public @Nullable List<String> getRecordedBy() {
    return recordedBy;
  }

  public void setRecordedBy(@Nullable List<String> recordedBy) {
    this.recordedBy = recordedBy;
  }

  @Override
  public @Nullable String getFieldNumber() {
    return fieldNumber;
  }

  public void setFieldNumber(@Nullable String fieldNumber) {
    this.fieldNumber = fieldNumber;
  }

  @Override
  public @Nullable String getRecordNumber() {
    return recordNumber;
  }

  public void setRecordNumber(@Nullable String recordNumber) {
    this.recordNumber = recordNumber;
  }

  @Override
  public @Nullable String getCatalogNumber() {
    return catalogNumber;
  }

  public void setCatalogNumber(@Nullable String catalogNumber) {
    this.catalogNumber = catalogNumber;
  }

  @Override
  public @Nullable List<String> getOtherCatalogNumbers() {
    return otherCatalogNumbers;
  }

  public void setOtherCatalogNumbers(@Nullable List<String> otherCatalogNumbers) {
    this.otherCatalogNumbers = otherCatalogNumbers;
  }

  @Override
  public @Nullable String getInstitutionCode() {
    return institutionCode;
  }

  public void setInstitutionCode(@Nullable String institutionCode) {
    this.institutionCode = institutionCode;
  }

  @Override
  public @Nullable String getCollectionCode() {
    return collectionCode;
  }

  public void setCollectionCode(@Nullable String collectionCode) {
    this.collectionCode = collectionCode;
  }
}
