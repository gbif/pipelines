package org.gbif.pipelines.core.parsers.clustering;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** A POJO implementation for simple tests. */
public class OccurrenceFeaturesPojo implements OccurrenceFeatures {
  private String id;
  private String datasetKey;
  private Integer speciesKey;
  private Integer taxonKey;
  private String basisOfRecord;
  private Double decimalLatitude;
  private Double decimalLongitude;
  private Integer year;
  private Integer month;
  private Integer day;
  private String eventDate;
  private String scientificName;
  private String countryCode;
  private String typeStatus;
  private String occurrenceID;
  private String recordedBy;
  private String fieldNumber;
  private String recordNumber;
  private String catalogNumber;
  private String otherCatalogNumbers;

  private OccurrenceFeaturesPojo(
      String id,
      String datasetKey,
      Integer speciesKey,
      Integer taxonKey,
      String basisOfRecord,
      Double decimalLatitude,
      Double decimalLongitude,
      Integer year,
      Integer month,
      Integer day,
      String eventDate,
      String scientificName,
      String countryCode,
      String typeStatus,
      String occurrenceID,
      String recordedBy,
      String fieldNumber,
      String recordNumber,
      String catalogNumber,
      String otherCatalogNumbers) {
    this.id = id;
    this.datasetKey = datasetKey;
    this.speciesKey = speciesKey;
    this.taxonKey = taxonKey;
    this.basisOfRecord = basisOfRecord;
    this.decimalLatitude = decimalLatitude;
    this.decimalLongitude = decimalLongitude;
    this.year = year;
    this.month = month;
    this.day = day;
    this.eventDate = eventDate;
    this.scientificName = scientificName;
    this.countryCode = countryCode;
    this.typeStatus = typeStatus;
    this.occurrenceID = occurrenceID;
    this.recordedBy = recordedBy;
    this.fieldNumber = fieldNumber;
    this.recordNumber = recordNumber;
    this.catalogNumber = catalogNumber;
    this.otherCatalogNumbers = otherCatalogNumbers;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getDatasetKey() {
    return datasetKey;
  }

  @Override
  public Integer getSpeciesKey() {
    return speciesKey;
  }

  @Override
  public Integer getTaxonKey() {
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
  public String getTypeStatus() {
    return typeStatus;
  }

  @Override
  public String getOccurrenceID() {
    return occurrenceID;
  }

  @Override
  public String getRecordedBy() {
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
  public String getOtherCatalogNumbers() {
    return otherCatalogNumbers;
  }

  @Override
  public List<String> getIdentifiers() {
    return Arrays.asList(
            getOccurrenceID(),
            getFieldNumber(),
            getRecordNumber(),
            getCatalogNumber(),
            getOtherCatalogNumbers())
        .stream()
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  static OccurrenceFeaturesPojoBuilder newBuilder() {
    return new OccurrenceFeaturesPojoBuilder();
  }

  static class OccurrenceFeaturesPojoBuilder {
    private String id;
    private String datasetKey;
    private Integer speciesKey;
    private Integer taxonKey;
    private String basisOfRecord;
    private Double decimalLatitude;
    private Double decimalLongitude;
    private Integer year;
    private Integer month;
    private Integer day;
    private String eventDate;
    private String scientificName;
    private String countryCode;
    private String typeStatus;
    private String occurrenceID;
    private String recordedBy;
    private String fieldNumber;
    private String recordNumber;
    private String catalogNumber;
    private String otherCatalogNumbers;

    public OccurrenceFeaturesPojoBuilder setId(String id) {
      this.id = id;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setDatasetKey(String datasetKey) {
      this.datasetKey = datasetKey;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setSpeciesKey(Integer speciesKey) {
      this.speciesKey = speciesKey;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setTaxonKey(Integer taxonKey) {
      this.taxonKey = taxonKey;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setBasisOfRecord(String basisOfRecord) {
      this.basisOfRecord = basisOfRecord;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setDecimalLatitude(Double decimalLatitude) {
      this.decimalLatitude = decimalLatitude;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setDecimalLongitude(Double decimalLongitude) {
      this.decimalLongitude = decimalLongitude;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setYear(Integer year) {
      this.year = year;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setMonth(Integer month) {
      this.month = month;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setDay(Integer day) {
      this.day = day;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setEventDate(String eventDate) {
      this.eventDate = eventDate;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setScientificName(String scientificName) {
      this.scientificName = scientificName;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setCountryCode(String countryCode) {
      this.countryCode = countryCode;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setTypeStatus(String typeStatus) {
      this.typeStatus = typeStatus;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setOccurrenceID(String occurrenceID) {
      this.occurrenceID = occurrenceID;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setRecordedBy(String recordedBy) {
      this.recordedBy = recordedBy;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setFieldNumber(String fieldNumber) {
      this.fieldNumber = fieldNumber;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setRecordNumber(String recordNumber) {
      this.recordNumber = recordNumber;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setCatalogNumber(String catalogNumber) {
      this.catalogNumber = catalogNumber;
      return this;
    }

    public OccurrenceFeaturesPojoBuilder setOtherCatalogNumbers(String otherCatalogNumbers) {
      this.otherCatalogNumbers = otherCatalogNumbers;
      return this;
    }

    public OccurrenceFeaturesPojo build() {
      return new OccurrenceFeaturesPojo(
          id,
          datasetKey,
          speciesKey,
          taxonKey,
          basisOfRecord,
          decimalLatitude,
          decimalLongitude,
          year,
          month,
          day,
          eventDate,
          scientificName,
          countryCode,
          typeStatus,
          occurrenceID,
          recordedBy,
          fieldNumber,
          recordNumber,
          catalogNumber,
          otherCatalogNumbers);
    }
  }
}
