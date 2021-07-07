package org.gbif.pipelines.core.parsers.clustering;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  String getInstitutionCode();

  String getCollectionCode();

  default List<String> listIdentifiers() {
    return Stream.of(
            getOccurrenceID(),
            getFieldNumber(),
            getRecordNumber(),
            getCatalogNumber(),
            getOtherCatalogNumbers(),
            getTripleIdentifier())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /** @return a triplet identifier of standard form ic:cc:cn when all triplets are present */
  default String getTripleIdentifier() {
    String[] codes = {getInstitutionCode(), getCollectionCode(), getCatalogNumber()};
    if (!Arrays.stream(codes).anyMatch(Objects::isNull)) {
      return String.join(":", codes);
    } else {
      return null;
    }
  }
}
