package org.gbif.pipelines.core.parsers.clustering;

import java.util.List;
import lombok.Builder;

/** A POJO implementation for simple tests. */
@Builder
public class OccurrenceFeaturesPojo implements OccurrenceFeatures {
  private final String id;
  private final String datasetKey;
  private final String speciesKey;
  private final String taxonKey;
  private final String basisOfRecord;
  private final Double decimalLatitude;
  private final Double decimalLongitude;
  private final Integer year;
  private final Integer month;
  private final Integer day;
  private final String eventDate;
  private final String scientificName;
  private final String countryCode;
  private final List<String> typeStatus;
  private final String occurrenceID;
  private final List<String> recordedBy;
  private final String fieldNumber;
  private final String recordNumber;
  private final String catalogNumber;
  private final List<String> otherCatalogNumbers;
  private final String institutionCode;
  private final String collectionCode;

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getDatasetKey() {
    return datasetKey;
  }

  @Override
  public String getSpeciesKey() {
    return speciesKey;
  }

  @Override
  public String getTaxonKey() {
    return taxonKey;
  }

  @Override
  public String getBasisOfRecord() {
    return basisOfRecord;
  }

  @Override
  public Double getDecimalLatitude() {
    return decimalLatitude;
  }

  @Override
  public Double getDecimalLongitude() {
    return decimalLongitude;
  }

  @Override
  public Integer getYear() {
    return year;
  }

  @Override
  public Integer getMonth() {
    return month;
  }

  @Override
  public Integer getDay() {
    return day;
  }

  @Override
  public String getEventDate() {
    return eventDate;
  }

  @Override
  public String getScientificName() {
    return scientificName;
  }

  @Override
  public String getCountryCode() {
    return countryCode;
  }

  @Override
  public List<String> getTypeStatus() {
    return typeStatus;
  }

  @Override
  public String getOccurrenceID() {
    return occurrenceID;
  }

  @Override
  public List<String> getRecordedBy() {
    return recordedBy;
  }

  @Override
  public String getFieldNumber() {
    return fieldNumber;
  }

  @Override
  public String getRecordNumber() {
    return recordNumber;
  }

  @Override
  public String getCatalogNumber() {
    return catalogNumber;
  }

  @Override
  public List<String> getOtherCatalogNumbers() {
    return otherCatalogNumbers;
  }

  @Override
  public String getInstitutionCode() {
    return institutionCode;
  }

  @Override
  public String getCollectionCode() {
    return collectionCode;
  }
}
