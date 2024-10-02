package org.gbif.pipelines.clustering;

import static org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships.concatIfEligible;
import static org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships.hashOrNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships;

/** Utility functions for hashing records to pre-group. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class HashUtilities {
  private static final Set<String> SPECIMEN_BASIS_OF_RECORD_SET =
      Set.of(
          "PRESERVED_SPECIMEN",
          "MATERIAL_SAMPLE",
          "LIVING_SPECIMEN",
          "FOSSIL_SPECIMEN",
          "MATERIAL_CITATION");

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
    Set<String> identifiers = hashCodesAndIDs(o, true);

    Set<Row> hashes = new HashSet<>();

    // generic grouping on species, time and space
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

    // identifiers overlap for the same species
    if (noNulls(speciesKey)) {
      for (String id : identifiers) {
        hashes.add(
            RowFactory.create(o.getId(), o.getDatasetKey(), String.join("|", speciesKey, id)));
      }
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

      // non-numeric identifiers for specimens used across datasets
      Set<String> codes = hashCodesAndIDs(o, true);
      for (String code : codes) {
        hashes.add(
            RowFactory.create(
                o.getId(),
                o.getDatasetKey(),
                String.join("|", o.getSpeciesKey(), OccurrenceRelationships.normalizeID(code))));
      }

      // stricter code hashing (non-numeric) but without species
      Set<String> codesStrict = hashCodesAndIDs(o, false);
      for (String code : codesStrict) {
        hashes.add(
            RowFactory.create(
                o.getId(),
                o.getDatasetKey(),
                String.join("|", OccurrenceRelationships.normalizeID(code))));
      }
    }
    return hashes;
  }

  /** Hashes all the various ways that record codes and identifiers are commonly used. */
  static Set<String> hashCodesAndIDs(OccurrenceFeatures o, boolean allowNumerics) {
    Stream<String> ids =
        Stream.of(
            hashOrNull(o.getOccurrenceID(), allowNumerics),
            hashOrNull(o.getRecordNumber(), allowNumerics),
            hashOrNull(o.getFieldNumber(), allowNumerics),
            hashOrNull(o.getCatalogNumber(), allowNumerics),
            concatIfEligible(
                ":", o.getInstitutionCode(), o.getCollectionCode(), o.getCatalogNumber()),
            concatIfEligible(":", o.getInstitutionCode(), o.getCatalogNumber()));
    if (o.getOtherCatalogNumbers() != null) {
      ids =
          Stream.concat(
              ids, o.getOtherCatalogNumbers().stream().map(c -> hashOrNull(c, allowNumerics)));
    }
    return ids.filter(OccurrenceRelationships::isEligibleCode).collect(Collectors.toSet());
  }

  /** Return true of no nulls or empty strings provided */
  static boolean noNulls(Object... o) {
    return Arrays.stream(o).noneMatch(s -> s == null || "".equals(s));
  }
}
