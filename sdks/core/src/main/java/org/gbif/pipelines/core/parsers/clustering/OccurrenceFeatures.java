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

  List<String> getTypeStatus();

  String getOccurrenceID();

  List<String> getRecordedBy();

  String getFieldNumber();

  String getRecordNumber();

  String getCatalogNumber();

  List<String> getOtherCatalogNumbers();

  String getInstitutionCode();

  String getCollectionCode();

  default List<String> listIdentifiers() {
    Stream<String> ids =
        Stream.of(
            getOccurrenceID(),
            getFieldNumber(),
            getRecordNumber(),
            getCatalogNumber(),
            getTripleIdentifier(),
            getScopedIdentifier());

    if (getOtherCatalogNumbers() != null) {
      ids = Stream.concat(ids, getOtherCatalogNumbers().stream());
    }

    return ids.filter(Objects::nonNull).collect(Collectors.toList());
  }

  /** @return a triplet identifier of standard form ic:cc:cn when all triplets are present */
  default String getTripleIdentifier() {
    String[] codes = {getInstitutionCode(), getCollectionCode(), getCatalogNumber()};
    if (Arrays.stream(codes).noneMatch(Objects::isNull)) {
      return String.join(":", codes);
    } else {
      return null;
    }
  }

  /** @return an identifier of form ic:cn when both are present */
  default String getScopedIdentifier() {
    String[] codes = {getInstitutionCode(), getCatalogNumber()};
    if (!Arrays.stream(codes).anyMatch(Objects::isNull)) {
      return String.join(":", codes);
    } else {
      return null;
    }
  }
}
