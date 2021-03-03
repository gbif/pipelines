package org.gbif.pipelines.core.parsers.clustering;

import java.util.List;

/**
 * The API to access the dimensions of an occurrence record necessary for clustering. Defined as an
 * interface to be portable across Spark Rows, Avro objects, POJOs etc.
 */
public interface OccurrenceFeatures {
  String getId();

  String getDatasetKey();

  String getSpeciesKey();

  String getTaxonKey();

  String getBasisOfRecord();

  Double getDecimalLatitude();

  Double getDecimalLongitude();

  Integer getYear();

  Integer getMonth();

  Integer getDay();

  String getEventDate();

  String getScientificName();

  String getCountryCode();

  String getTypeStatus();

  String getOccurrenceID();

  String getRecordedBy();

  String getFieldNumber();

  String getRecordNumber();

  String getCatalogNumber();

  String getOtherCatalogNumbers();

  List<String> listIdentifiers();
}
