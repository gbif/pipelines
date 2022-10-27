package org.gbif.pipelines.core.parsers.clustering;

import static org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion.FeatureAssertion.*;
import static org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion.FeatureAssertion.IDENTIFIERS_OVERLAP;

import com.google.common.annotations.VisibleForTesting;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion.FeatureAssertion;

/** Generates relationship assertions for occurrence records. */
public class OccurrenceRelationships {
  private static final String REGEX_IDENTIFIERS =
      "[-.*,_ :|/\\\\#%&]"; // chars to remove from identifiers

  private static final int THRESHOLD_IN_DAYS = 1;

  // A list of IDs that are excluded for comparison
  private static final Set<String> idOmitList = newIdOmitList();

  private static final Set<String> SPECIMEN_BORS =
      new HashSet<>(
          Arrays.asList(
              "PRESERVED_SPECIMEN", "LIVING_SPECIMEN", "FOSSIL_SPECIMEN", "MATERIAL_CITATION"));

  /** Will either generate an assertion with justification or return null. */
  public static <T extends OccurrenceFeatures> RelationshipAssertion<T> generate(T o1, T o2) {

    RelationshipAssertion<T> assertion = new RelationshipAssertion<>(o1, o2);

    // a rule based approach which could port to e.g. easy-rules if this approach is to grow

    // generate "facts"
    compareTaxa(o1, o2, assertion);
    compareIdentifiers(o1, o2, assertion);
    compareCatalogNumbers(o1, o2, assertion); // more specific than identifiers
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
    List<FeatureAssertion[]> passConditions =
        new ArrayList<>(
            Arrays.asList(
                new FeatureAssertion[][] {
                  {SAME_ACCEPTED_SPECIES, SAME_COORDINATES, SAME_DATE},
                  {SAME_ACCEPTED_SPECIES, WITHIN_200m, SAME_DATE},
                  {
                    SAME_ACCEPTED_SPECIES,
                    SAME_COORDINATES,
                    NON_CONFLICTING_DATE,
                    IDENTIFIERS_OVERLAP
                  },
                  {SAME_ACCEPTED_SPECIES, WITHIN_200m, NON_CONFLICTING_DATE, IDENTIFIERS_OVERLAP},
                  {SAME_ACCEPTED_SPECIES, WITHIN_2Km, SAME_DATE, IDENTIFIERS_OVERLAP},
                  {SAME_ACCEPTED_SPECIES, WITHIN_2Km, NON_CONFLICTING_DATE, IDENTIFIERS_OVERLAP},
                  {
                    SAME_ACCEPTED_SPECIES,
                    NON_CONFLICTING_COORDINATES,
                    SAME_DATE,
                    IDENTIFIERS_OVERLAP
                  },
                  {SAME_ACCEPTED_SPECIES, SAME_COORDINATES, APPROXIMATE_DATE, SAME_RECORDER_NAME},
                  {SAME_ACCEPTED_SPECIES, WITHIN_2Km, APPROXIMATE_DATE, SAME_RECORDER_NAME},
                }));

    // Accommodate sparse data from sequence repositories
    // see https://github.com/gbif/pipelines/issues/733
    if (o1.isFromSequenceRepository() || o2.isFromSequenceRepository()) {
      passConditions.add(
          new FeatureAssertion[] {
            SAME_ACCEPTED_SPECIES,
            NON_CONFLICTING_COORDINATES,
            NON_CONFLICTING_DATE,
            IDENTIFIERS_OVERLAP
          });
    }

    // Relax rules to accommodate well-formed otherCatalogNumber assertions
    // See https://github.com/gbif/pipelines/issues/781
    if (SPECIMEN_BORS.contains(o1.getBasisOfRecord())
        && SPECIMEN_BORS.contains(o2.getBasisOfRecord())) {
      passConditions.add(new FeatureAssertion[] {OTHER_CATALOG_NUMBERS_OVERLAP});
    }

    // always exclude things on different location or date
    if (assertion.justificationDoesNotContain(DIFFERENT_DATE, DIFFERENT_COUNTRY)) {
      // for any ruleset that matches we generate the assertion
      for (FeatureAssertion[] conditions : passConditions) {
        if (assertion.justificationContainsAll(conditions)) {
          return assertion;
        }
      }
    }

    return null;
  }

  /**
   * A specimen is the same if it is the holotype of the same species. Other cases may be added, but
   * difficult to be 100% sure.
   */
  private static <T extends OccurrenceFeatures> void assertSameSpecimen(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion<T> assertion) {
    if (equalsAndNotNull(o1.getTaxonKey(), o2.getTaxonKey())
        && containsIgnoreCase(o1.getTypeStatus(), "HOLOTYPE")
        && containsIgnoreCase(o2.getTypeStatus(), "HOLOTYPE")) {
      assertion.collect(SAME_SPECIMEN);
    }
  }

  private static <T extends OccurrenceFeatures> void assertTypification(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion<T> assertion) {
    if (equalsAndNotNull(o1.getScientificName(), o2.getScientificName())
        && notEmpty(o1.getTypeStatus())
        && notEmpty(o2.getTypeStatus())) {
      assertion.collect(TYPIFICATION_RELATION);
    }
  }

  private static <T extends OccurrenceFeatures> void compareTaxa(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion<T> assertion) {
    if (equalsAndNotNull(o1.getSpeciesKey(), o2.getSpeciesKey())) {
      assertion.collect(SAME_ACCEPTED_SPECIES);
    }
  }

  private static <T extends OccurrenceFeatures> void compareDates(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion<T> assertion) {

    // verbosely written with readability in mind
    if (equalsAndNotNull(o1.getYear(), o2.getYear())
        && equalsAndNotNull(o1.getMonth(), o2.getMonth())
        && equalsAndNotNull(o1.getDay(), o2.getDay())) {
      assertion.collect(SAME_DATE);

    } else if (equalsAndNotNull(o1.getEventDate(), o2.getEventDate())) {
      assertion.collect(SAME_DATE);

    } else if (withinDays(o1, o2)) {
      // accommodate records 1 day apart for e.g. start and end day of an overnight trap, or a
      // timezone issue
      assertion.collect(APPROXIMATE_DATE);

    } else if (presentAndNotEquals(o1.getEventDate(), o2.getEventDate())) {
      assertion.collect(DIFFERENT_DATE);

    } else if (allNull(
        o1.getEventDate(),
        o1.getDay(),
        o1.getMonth(),
        o1.getYear(),
        o2.getEventDate(),
        o2.getDay(),
        o2.getMonth(),
        o2.getYear())) {
      // no date on either record
      assertion.collect(NON_CONFLICTING_DATE);

    } else if (presentOnOneOnly(o1.getEventDate(), o2.getEventDate())) {
      // only one has a date (note that an eventDate is always materialised for a D/M/Y)
      assertion.collect(NON_CONFLICTING_DATE);
    }
  }

  /**
   * @return true if o1 and o2 are collected with threshold days (e.g. 12/3/2020 and 13/3/2020 are 1
   *     day apart)
   */
  private static boolean withinDays(OccurrenceFeatures o1, OccurrenceFeatures o2) {
    if (o1.getYear() != null
        && o1.getMonth() != null
        && o1.getDay() != null
        && o2.getYear() != null
        && o2.getMonth() != null
        && o2.getDay() != null) {
      LocalDate d1 = LocalDate.of(o1.getYear(), o1.getMonth(), o1.getDay());
      LocalDate d2 = LocalDate.of(o2.getYear(), o2.getMonth(), o2.getDay());
      int daysApart = Math.abs(d1.until(d2).getDays());
      return daysApart <= THRESHOLD_IN_DAYS;
    }
    return false;
  }

  private static <T extends OccurrenceFeatures> void compareCollectors(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion<T> assertion) {
    if (intersectWithValues(o1.getRecordedBy(), o2.getRecordedBy())) {
      // this could be improved with similarity checks
      assertion.collect(SAME_RECORDER_NAME);
    }
  }

  private static <T extends OccurrenceFeatures> void compareCoordinates(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion<T> assertion) {
    if (equalsAndNotNull(o1.getDecimalLatitude(), o2.getDecimalLatitude())
        && equalsAndNotNull(o1.getDecimalLongitude(), o2.getDecimalLongitude())) {
      assertion.collect(SAME_COORDINATES);
    } else if (allNull(
            o1.getDecimalLatitude(),
            o1.getDecimalLongitude(),
            o2.getDecimalLatitude(),
            o2.getDecimalLongitude())
        || (presentOnOneOnly(o1.getDecimalLatitude(), o2.getDecimalLatitude())
            && presentOnOneOnly(o1.getDecimalLongitude(), o2.getDecimalLongitude()))) {
      // all null or null on one side
      assertion.collect(NON_CONFLICTING_COORDINATES);
    } else if (presentOnBoth(o1.getDecimalLatitude(), o2.getDecimalLatitude())
        && presentOnBoth(o1.getDecimalLongitude(), o2.getDecimalLongitude())) {
      double distance =
          Haversine.distance(
              o1.getDecimalLatitude(),
              o1.getDecimalLongitude(),
              o2.getDecimalLatitude(),
              o2.getDecimalLongitude());

      if (distance <= 0.200) {
        assertion.collect(WITHIN_200m); // 157m is 3 decimal places
      }
      if (distance <= 2.00) {
        assertion.collect(WITHIN_2Km); // 1569m is worst 3 decimal places
      }
    }
  }

  private static <T extends OccurrenceFeatures> void compareCountry(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion<T> assertion) {
    if (equalsAndNotNull(o1.getCountryCode(), o2.getCountryCode())) {
      assertion.collect(SAME_COUNTRY);
    } else if (presentOnOneOnly(o1.getCountryCode(), o2.getCountryCode())) {
      assertion.collect(NON_CONFLICTING_COUNTRY);
    } else if (presentAndNotEquals(o1.getCountryCode(), o2.getCountryCode())) {
      assertion.collect(DIFFERENT_COUNTRY);
    }
  }

  @VisibleForTesting
  static <T extends OccurrenceFeatures> void compareIdentifiers(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion<T> assertion) {
    // ignore case and [-_., ] chars
    Set<String> intersection =
        o1.listIdentifiers().stream()
            .map(OccurrenceRelationships::normalizeID)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

    Set<String> toMatch =
        o2.listIdentifiers().stream()
            .map(OccurrenceRelationships::normalizeID)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

    intersection.retainAll(toMatch);
    Set<String> filtered =
        intersection.stream().filter(c -> !idOmitList.contains(c)).collect(Collectors.toSet());

    if (!filtered.isEmpty()) {
      assertion.collect(IDENTIFIERS_OVERLAP);
    }
  }

  /**
   * Detects if either of the catalogNumbers formatted in various ways (CN, IC:CN and IC:CC:CN) is
   * present in the other records otherCatalogNumber. This is intended to detect clearly defined
   * assertion where one publisher provides e.g. otherCatalogNumber=KU:MAMM:X123 and the other
   * record carries those values in the individual fields.
   */
  static <T extends OccurrenceFeatures> void compareCatalogNumbers(
      OccurrenceFeatures o1, OccurrenceFeatures o2, RelationshipAssertion<T> assertion) {

    if (catalogNumberOverlaps(
            o1.getInstitutionCode(),
            o1.getCollectionCode(),
            o1.getCatalogNumber(),
            o2.getOtherCatalogNumbers())
        || catalogNumberOverlaps(
            o2.getInstitutionCode(),
            o2.getCollectionCode(),
            o2.getCatalogNumber(),
            o1.getOtherCatalogNumbers())) {
      assertion.collect(OTHER_CATALOG_NUMBERS_OVERLAP);
    }
  }

  /**
   * Returns true if the ic:cc:cn and provided target overlaps, after formatting rules are applied:
   *
   * <ol>
   *   <li>The target codes may not start with the common Cat. or Cat# prefixes (ignoring case)
   *   <li>Whitespace and delimited characters such as :/_ etc are ignored in the comparison
   * </ol>
   */
  static boolean catalogNumberOverlaps(String ic, String cc, String cn, List<String> target) {
    if (target == null) return false;

    Set<String> codes =
        Stream.of(concatIfEligible(":", ic, cc, cn))
            .map(OccurrenceRelationships::normalizeID)
            .filter(c -> isEligibleCode(c) && !isNumeric(c))
            .collect(Collectors.toSet());

    Set<String> targetCodes =
        target.stream()
            .map(c -> c.replaceFirst("^[Cc]at[.#]", "")) // remove common prefixes of Cat. Cat#
            .map(OccurrenceRelationships::normalizeID)
            .filter(c -> isEligibleCode(c) && !isNumeric(c))
            .collect(Collectors.toSet());

    targetCodes.retainAll(codes);
    return !targetCodes.isEmpty();
  }

  static boolean equalsAndNotNull(Object o1, Object o2) {
    return o1 != null && Objects.equals(o1, o2);
  }

  static boolean presentAndNotEquals(Object o1, Object o2) {
    return o1 != null && o2 != null && !Objects.equals(o1, o2);
  }

  static boolean presentOnOneOnly(Object o1, Object o2) {
    return (o1 == null && o2 != null) || (o1 != null && o2 == null);
  }

  static boolean allNull(Object... o1) {
    return o1 == null || Arrays.stream(o1).allMatch(o -> o == null);
  }

  static boolean presentOnBoth(Object o1, Object o2) {
    return o1 != null && o2 != null;
  }

  static boolean intersectWithValues(List<String> o1, List<String> o2) {
    if (o1 != null && o2 != null) {
      return !o1.stream()
          .distinct()
          .filter(v -> containsIgnoreCase(o2, v))
          .collect(Collectors.toSet())
          .isEmpty();
    }
    return false;
  }

  static boolean containsIgnoreCase(List<String> list, String value) {
    return list != null && list.stream().anyMatch(value::equalsIgnoreCase);
  }

  static boolean notEmpty(List<?> list) {
    return list != null && !list.isEmpty();
  }

  public static String normalizeID(String id) {
    if (id != null) {
      String n = id.toUpperCase().replaceAll(REGEX_IDENTIFIERS, "");
      return n.length() == 0 ? null : n;
    }
    return null;
  }

  /** Returns a concatenated string only when all atoms are eligible, otherwise null. */
  public static String concatIfEligible(String separator, String... s) {
    if (Arrays.stream(s).allMatch(a -> isEligibleCode(a))) {
      return String.join(
          separator,
          Arrays.stream(s).map(OccurrenceRelationships::normalizeID).collect(Collectors.toList()));
    } else {
      return null;
    }
  }

  /** Return the code if eligible or null */
  public static String hashOrNull(String code, boolean allowNumeric) {
    if (allowNumeric) {
      return isEligibleCode(code) ? OccurrenceRelationships.normalizeID(code) : null;
    } else {
      return isEligibleCode(code) && !isNumeric(code)
          ? OccurrenceRelationships.normalizeID(code)
          : null;
    }
  }

  /** Return true if the code is not in the exclusion list or null. */
  public static boolean isEligibleCode(String code) {
    if (code == null
        || code.length() == 0
        || idOmitList.contains(OccurrenceRelationships.normalizeID(code))) {
      return false;
    } else {
      return true;
    }
  }

  public static boolean isNumeric(String s) {
    if (s == null) return false;
    try {
      Double.parseDouble(s);
      return true;
    } catch (NumberFormatException nfe) {
      return false;
    }
  }

  /** Creates a new exclusion list for IDs. See https://github.com/gbif/pipelines/issues/309. */
  public static Set<String> newIdOmitList() {
    return new HashSet(
        Arrays.asList(
            null,
            "",
            "[]",
            "*",
            "--",
            normalizeID("NO APLICA"),
            normalizeID("NA"),
            normalizeID("NO DISPONIBLE"),
            normalizeID("NO DISPONIBL"),
            normalizeID("NO NUMBER"),
            normalizeID("UNKNOWN"),
            normalizeID("SN"),
            normalizeID("ANONYMOUS"),
            normalizeID("NONE"),
            normalizeID("s.n."),
            normalizeID("Unknown s.n."),
            normalizeID("Unreadable s.n."),
            normalizeID("se kommentar"),
            normalizeID("inget id"),
            normalizeID("x"),
            normalizeID("Anonymous s.n."),
            normalizeID("Collector Number: s.n."),
            normalizeID("No Number"),
            normalizeID("Anonymous"),
            normalizeID("None"),
            normalizeID("No Field Number"),
            normalizeID("not recorded"),
            normalizeID("s.l."),
            normalizeID("s.c."),
            normalizeID("present"),
            normalizeID("Undef/entomo"),
            normalizeID("s/nº"),
            normalizeID("undef"),
            normalizeID("no data")));
  }
}
