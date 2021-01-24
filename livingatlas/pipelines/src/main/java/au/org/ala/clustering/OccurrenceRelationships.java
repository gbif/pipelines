package au.org.ala.clustering;

import static au.org.ala.clustering.RelationshipAssertion.FEATURE_ASSERTION.*;

import java.time.LocalDate;
import java.util.*;
import org.gbif.pipelines.io.avro.OccurrenceFeatures;

/** Generates relationship assertions for occurrence records. */
public class OccurrenceRelationships {

  private static final String REGEX_IDENTIFIERS =
      "[-.,_ :|/\\\\#%&]"; // chars to remove from identifiers

  /** Will either generate an assertion with justification or return null. */
  public static RelationshipAssertion generate(OccurrenceFeatures o1, OccurrenceFeatures o2) {

    RelationshipAssertion assertion = new RelationshipAssertion(o1, o2);

    // a rule based approach which could port to e.g. easy-rules if this approach is to grow

    // generate "facts"
    compareTaxa(o1, o2, assertion);
    compareIdentifiers(o1, o2, assertion);
    compareDates(o1, o2, assertion);
    compareCollectors(o1, o2, assertion);
    compareCoordinates(o1, o2, assertion);
    compareCountry(o1, o2, assertion);
    assertSameSpecimen(o1, o2, assertion);
    assertTypification(o1, o2, assertion);

    // short circuit: typification events and duplicate specimens are always of interest
    if (assertion.justificationContains(SAME_SPECIMEN)
        || assertion.justificationContains(TYPIFICATION_RELATION)) {
      return assertion;
    }

    // fact combinations that are of interest as assertions
    RelationshipAssertion.FEATURE_ASSERTION[][] passConditions = {
      {SAME_ACCEPTED_SPECIES, SAME_COORDINATES, SAME_DATE},
      {SAME_ACCEPTED_SPECIES, WITHIN_200m, SAME_DATE}, // accommodate 3 decimal place roundings
      {SAME_ACCEPTED_SPECIES, SAME_COORDINATES, NON_CONFLICTING_DATE, IDENTIFIERS_OVERLAP},
      {SAME_ACCEPTED_SPECIES, WITHIN_200m, NON_CONFLICTING_DATE, IDENTIFIERS_OVERLAP},
      {SAME_ACCEPTED_SPECIES, WITHIN_2Km, SAME_DATE, IDENTIFIERS_OVERLAP},
      {SAME_ACCEPTED_SPECIES, WITHIN_2Km, NON_CONFLICTING_DATE, IDENTIFIERS_OVERLAP},
      {SAME_ACCEPTED_SPECIES, NON_CONFLICTING_COORDINATES, SAME_DATE, IDENTIFIERS_OVERLAP},
      {
        SAME_ACCEPTED_SPECIES,
        NON_CONFLICTING_COORDINATES,
        NON_CONFLICTING_DATE,
        IDENTIFIERS_OVERLAP
      },
      {SAME_ACCEPTED_SPECIES, SAME_COORDINATES, APPROXIMATE_DATE, SAME_RECORDER_NAME},
      {SAME_ACCEPTED_SPECIES, WITHIN_2Km, APPROXIMATE_DATE, SAME_RECORDER_NAME},
    };

    // always exclude things on different location or date
    if (assertion.justificationDoesNotContain(DIFFERENT_DATE, DIFFERENT_COUNTRY)) {
      // for any ruleset that matches we generate the assertion
      for (RelationshipAssertion.FEATURE_ASSERTION[] conditions : passConditions) {
        if (assertion.justificationContainsAll(conditions)) return assertion;
      }
    }

    return null;
  }

  /**
   * A specimen is the same if it is the hototype of the same species. Other cases may be added, but
   * difficult to be 100% sure.
   */
  private static void assertSameSpecimen(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion assertion) {
    if (equalsAndNotNull(o1, o2, "taxonKey")
        && equalsAndNotNull(o1, o2, "typeStatus")
        && "HOLOTYPE".equalsIgnoreCase(o1.getTypeStatus())) {
      assertion.collect(SAME_SPECIMEN);
    }
  }

  private static void assertTypification(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion assertion) {
    if (equalsAndNotNull(o1, o2, "scientificName") && presentOnBoth(o1, o2, "typeStatus")) {
      assertion.collect(TYPIFICATION_RELATION);
    }
  }

  private static void compareTaxa(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion assertion) {
    if (equalsAndNotNull(o1, o2, "speciesKey")) {
      assertion.collect(SAME_ACCEPTED_SPECIES);
    }
  }

  private static void compareDates(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion assertion) {
    if (equalsAndNotNull(o1, o2, "year")
        && equalsAndNotNull(o1, o2, "month")
        && equalsAndNotNull(o1, o2, "day")) {
      assertion.collect(SAME_DATE);
    } else if (equalsAndNotNull(o1, o2, "eventDate")) {
      assertion.collect(SAME_DATE);
    } else if (presentOnOneOnly(o1, o2, "eventDate")) {
      assertion.collect(NON_CONFLICTING_DATE);
    } else if (withinDays(o1, o2, 1)) {
      // accommodate records 1 day apart for e.g. start and end day of an overnight trap, or a
      // timezone issue
      assertion.collect(APPROXIMATE_DATE);
    } else if (presentAndNotEquals(o1, o2, "eventDate")) {
      assertion.collect(DIFFERENT_DATE);
    }
  }

  /**
   * @return the true if o1 and o2 are collected with threshold days (e.g. 12/3/2020 and 13/3/2020
   *     are 1 day apart)
   */
  private static boolean withinDays(
      OccurrenceFeatures o1, OccurrenceFeatures o2, int thresholdInDays) {
    if (o1.getYear() != null
        && o1.getMonth() != null
        && o1.getDay() != null
        && o2.getYear() != null
        && o2.getMonth() != null
        && o2.getDay() != null) {
      LocalDate d1 = LocalDate.of(o1.getYear(), o1.getMonth(), o1.getDay());
      LocalDate d2 = LocalDate.of(o2.getYear(), o2.getMonth(), o2.getDay());
      int daysApart = Math.abs(d1.until(d2).getDays());
      return daysApart <= thresholdInDays;
    }
    return false;
  }

  private static void compareCollectors(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion assertion) {
    if (equalsAndNotNull(o1, o2, "recordedBy")) {
      // this could be improved with parsing and similarity checks
      assertion.collect(SAME_RECORDER_NAME);
    }
  }

  private static void compareCoordinates(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion assertion) {
    if (equalsAndNotNull(o1, o2, "decimalLatitude")
        && equalsAndNotNull(o1, o2, "decimalLongitude")) {
      assertion.collect(SAME_COORDINATES);
    } else if (presentOnOneOnly(o1, o2, "decimalLatitude")
        && presentOnOneOnly(o1, o2, "decimalLongitude")) {
      assertion.collect(NON_CONFLICTING_COORDINATES);
    } else if (presentOnBoth(o1, o2, "decimalLatitude", "decimalLongitude")) {
      double distance =
          Haversine.distance(
              o1.getDecimalLatitude(),
              o1.getDecimalLongitude(),
              o2.getDecimalLatitude(),
              o2.getDecimalLongitude());

      if (distance <= 0.200) assertion.collect(WITHIN_200m); // 157m is 3 decimal places
      if (distance <= 2.00) assertion.collect(WITHIN_2Km); // 1569m is worst 3 decimal places
    }
  }

  private static void compareCountry(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion assertion) {
    if (equalsAndNotNull(o1, o2, "countryCode")) {
      assertion.collect(SAME_COUNTRY);
    } else if (presentOnOneOnly(o1, o2, "countryCode")) {
      assertion.collect(NON_CONFLICTING_COUNTRY);
    } else if (presentAndNotEquals(o1, o2, "countryCode")) {
      assertion.collect(DIFFERENT_COUNTRY);
    }
  }

  private static void compareIdentifiers(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion assertion) {
    // ignore case and [-_., ] chars
    // otherCatalogNumbers is not parsed, but a good addition could be to explore that
    Set<String> intersection = new HashSet();
    List<String> identifiers =
        Arrays.asList(
            o1.getFieldNumber(),
            o1.getRecordNumber(),
            o1.getCatalogNumber(),
            o1.getOtherCatalogNumbers(),
            o1.getOccurrenceID());

    identifiers.forEach(
        id -> {
          if (id != null) {
            intersection.add(normalizeID(id));
          }
        });

    Set<String> toMatch = new HashSet();
    identifiers.forEach(
        id -> {
          if (id != null) {
            toMatch.add(normalizeID(id));
          }
        });

    intersection.retainAll(toMatch);

    // See https://github.com/gbif/pipelines/issues/309
    intersection.removeAll(
        Arrays.asList(
            null,
            "",
            "NOAPLICA",
            "NA",
            "[]",
            "NODISPONIBLE",
            "NODISPONIBL",
            "NONUMBER",
            "--",
            "UNKNOWN"));

    if (!intersection.isEmpty()) {
      assertion.collect(IDENTIFIERS_OVERLAP);
    }
  }

  static boolean equalsAndNotNull(OccurrenceFeatures o1, OccurrenceFeatures o2, String field) {
    return o1.get(field) != null && Objects.equals(o1.get(field), o2.get(field));
  }

  static boolean presentAndNotEquals(OccurrenceFeatures o1, OccurrenceFeatures o2, String field) {
    return o1.get(field) != null && !Objects.equals(o1.get(field), o2.get(field));
  }

  static boolean presentOnOneOnly(OccurrenceFeatures o1, OccurrenceFeatures o2, String field) {
    return (o1.get(field) == null && o1.get(field) != null)
        || (o1.get(field) != null && o1.get(field) == null);
  }

  static boolean presentOnBoth(OccurrenceFeatures o1, OccurrenceFeatures o2, String... fields) {
    for (String f : fields) {
      if (o1.get(f) == null) return false;
      if (o2.get(f) == null) return false;
    }
    return true;
  }

  public static String normalizeID(String id) {
    if (id != null) {
      String n = id.toUpperCase().replaceAll(REGEX_IDENTIFIERS, "");
      return n.length() == 0 ? null : n;
    }
    return null;
  }
}
