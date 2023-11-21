package org.gbif.pipelines.clustering;

import static org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships.concatIfEligible;
import static org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships.hashOrNull;
import static org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships.isEligibleCode;
import static org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships.isNumeric;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships;

/** Utility functions for hashing records to pre-group. */
class HashUtilities {
  private static final Set<String> SPECIMEN_BASIS_OF_RECORD_SET =
      Stream.of(
              "PRESERVED_SPECIMEN",
              "MATERIAL_SAMPLE",
              "LIVING_SPECIMEN",
              "FOSSIL_SPECIMEN",
              "MATERIAL_CITATION")
          .collect(Collectors.toSet());

  static Iterator<Row> recordHashes(OccurrenceFeatures o) {
    Double lat = o.getDecimalLatitude();
    Double lng = o.getDecimalLongitude();
    Integer year = o.getYear();
    Integer month = o.getMonth();
    Integer day = o.getDay();
    String taxonKey = o.getTaxonKey();
    List<String> typeStatus = o.getTypeStatus();
    List<String> recordedBy = o.getRecordedBy();
    String speciesKey = o.getSpeciesKey();

    Set<Row> hashes = new HashSet<>();

    // generic grouping
    if (noNulls(lat, lng, year, month, day, speciesKey)) {
      hashes.add(
          RowFactory.create(
              o.getId(),
              o.getDatasetKey(),
              String.join(
                  "|",
                  speciesKey,
                  String.valueOf(Math.round(lat * 1000)),
                  String.valueOf(Math.round(lng * 1000)),
                  year.toString(),
                  month.toString(),
                  day.toString())));
    }

    // anything claiming a type for the same name is of interest (regardless of type stated)
    if (noNulls(taxonKey, typeStatus) && !typeStatus.isEmpty())
      hashes.add(
          RowFactory.create(o.getId(), o.getDatasetKey(), String.join("|", taxonKey, "TYPE")));

    // all similar species recorded by the same person within the same year are of interest
    if (noNulls(taxonKey, year, recordedBy)) {
      for (String r : recordedBy) {
        hashes.add(
            RowFactory.create(o.getId(), o.getDatasetKey(), String.join("|", year.toString(), r)));
      }
    }

    // append the specimen specific hashes
    hashes.addAll(specimenHashes(o));

    return hashes.iterator();
  }

  /**
   * Generate hashes for specimens combining the various IDs and accepted species. Specimens often
   * link by record identifiers, while other occurrence data skews here greatly for little benefit.
   */
  private static Set<Row> specimenHashes(OccurrenceFeatures o) {
    Set<Row> hashes = new HashSet<>();
    String bor = o.getBasisOfRecord();
    if (SPECIMEN_BASIS_OF_RECORD_SET.contains(bor)) {
      // extract all the IDs we can think of, hashed with the species key
      Stream<String> ids =
          Stream.of(
              hashOrNull(o.getOccurrenceID(), true),
              hashOrNull(o.getRecordNumber(), true),
              hashOrNull(o.getFieldNumber(), true),
              hashOrNull(o.getCatalogNumber(), true),
              concatIfEligible(
                  ":", o.getInstitutionCode(), o.getCollectionCode(), o.getCatalogNumber()),
              concatIfEligible(":", o.getInstitutionCode(), o.getCatalogNumber()));
      if (o.getOtherCatalogNumbers() != null) {
        ids = Stream.concat(ids, o.getOtherCatalogNumbers().stream().map(c -> hashOrNull(c, true)));
      }
      ids = ids.filter(c -> isEligibleCode(c));

      // hashed using codes (including numeric) and species
      ids.forEach(
          id -> {
            hashes.add(
                RowFactory.create(
                    o.getId(),
                    o.getDatasetKey(),
                    String.join("|", o.getSpeciesKey(), OccurrenceRelationships.normalizeID(id))));
          });

      // stricter code hashing (non-numeric) but without species
      Stream<String> catalogNumbers = hashCatalogNumbers(o);
      catalogNumbers.forEach(
          cat -> {
            hashes.add(RowFactory.create(o.getId(), o.getDatasetKey(), cat));
          });
    }
    return hashes;
  }

  /**
   * Hashes the occurrenceID, catalogCode and otherCatalogNumber into a Set of codes. The
   * catalogNumber is constructed as it's code and also prefixed in the commonly used format of
   * IC:CC:CN and IC:CN.
   *
   * @return a set of hashes suitable for grouping specimen related records without other filters.
   */
  static Stream<String> hashCatalogNumbers(OccurrenceFeatures o) {
    Stream<String> cats =
        Stream.of(
            hashOrNull(o.getCatalogNumber(), false),
            hashOrNull(o.getOccurrenceID(), false),
            concatIfEligible(
                ":", o.getInstitutionCode(), o.getCollectionCode(), o.getCatalogNumber()),
            concatIfEligible(":", o.getInstitutionCode(), o.getCatalogNumber()));

    if (o.getOtherCatalogNumbers() != null) {
      cats =
          Stream.concat(cats, o.getOtherCatalogNumbers().stream().map(c -> hashOrNull(c, false)));
    }
    return cats.map(OccurrenceRelationships::normalizeID)
        .filter(c -> isEligibleCode(c) && !isNumeric(c));
  }

  /** Return true of no nulls or empty strings provided */
  static boolean noNulls(Object... o) {
    return Arrays.stream(o).noneMatch(s -> s == null || "".equals(s));
  }
}
