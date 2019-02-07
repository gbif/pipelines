package org.gbif.pipelines.parsers.parsers.location;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.parsers.location.legacy.CountryMaps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH;
import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES;

/** Matches the location fields related to Country and Coordinates to find possible mismatches. */
class LocationMatcher {

  private static final Logger LOG = LoggerFactory.getLogger(LocationMatcher.class);

  // Antarctica: "Territories south of 60° south latitude"
  private static final double ANTARCTICA_LATITUDE = -60d;

  private final LatLng latLng;
  private final Country country;
  private final KeyValueStore<org.gbif.kvs.geocode.LatLng, String> kvStore;
  private final List<Function<LatLng, LatLng>> alternativeTransformations = new ArrayList<>();

  private LocationMatcher(LatLng latLng, Country country, KeyValueStore<org.gbif.kvs.geocode.LatLng, String> kvStore) {
    this.latLng = latLng;
    this.country = country;
    this.kvStore = kvStore;
  }

  static LocationMatcher create(LatLng latLng, Country country, KeyValueStore<LatLng, String> kvStore) {
    return new LocationMatcher(latLng, country, kvStore);
  }

  LocationMatcher additionalTransform(Function<LatLng, LatLng> transformation) {
    alternativeTransformations.add(transformation);
    return this;
  }

  ParsedField<ParsedLocation> apply() {
    // Check parameters
    Objects.requireNonNull(latLng);
    if (latLng.getLatitude() == null || latLng.getLongitude() == null) {
      throw new IllegalArgumentException("Empty coordinates");
    }

    // Match country
    return country != null ? applyWithCountry() : applyWithoutCountry();
  }

  private ParsedField<ParsedLocation> applyWithCountry() {

    Optional<Country> countryKv = getCountryFromCoordinates(latLng);

    // if the WS returned countries we try to match with them
    if (countryKv.isPresent()) {
      Country c = countryKv.get();
      if (c.equals(this.country)) {
        // country found
        return success(this.country, latLng);
      }

      // if not found, try with equivalent countries
      Optional<Country> equivalentMatch =
          containsAnyCountry(CountryMaps.equivalent(this.country), Collections.singletonList(c));
      if (equivalentMatch.isPresent()) {
        // country found
        return success(equivalentMatch.get(), latLng);
      }

      // if not found, try with confused countries
      Optional<Country> confusedMatch =
          containsAnyCountry(CountryMaps.confused(this.country), Collections.singletonList(c));
      if (confusedMatch.isPresent()) {
        // country found
        return success(confusedMatch.get(), latLng, COUNTRY_DERIVED_FROM_COORDINATES);
      }
    }

    // if still not found, try alternatives
    for (Function<LatLng, LatLng> transformation : alternativeTransformations) {
      // transform location
      LatLng latLngTransformed = transformation.apply(latLng);

      // call ws
      Optional<Country> countriesFound = getCountryFromCoordinates(latLngTransformed);
      if (countriesFound.filter(x -> x.equals(country)).isPresent()) {
        // country found
        // Add issues from the transformation
        return success(country, latLngTransformed, CoordinatesFunction.getIssueTypes(transformation));
      }
    }

    // no result found
    return fail();
  }

  private ParsedField<ParsedLocation> applyWithoutCountry() {
    // call WS with identity coords
    return getCountryFromCoordinates(latLng)
        .map(x -> success(x, latLng, COUNTRY_DERIVED_FROM_COORDINATES))
        .orElse(fail());
  }

  private Optional<Country> getCountryFromCoordinates(LatLng latLng) {
    if (latLng.isValid()) {
      String countryKv;
      try {
        countryKv = kvStore.get(latLng);
      } catch (NoSuchElementException | NullPointerException e) {
        countryKv = "";
      }
      if (!Strings.isNullOrEmpty(countryKv)) {
        return Optional.of(Country.fromIsoCode(countryKv));
      }
      if (Strings.isNullOrEmpty(countryKv) && isAntarctica(latLng.getLatitude(), this.country)) {
        return Optional.of(Country.ANTARCTICA);
      }
    }
    return Optional.empty();
  }

  private static Optional<Country> containsAnyCountry(Set<Country> possibilities, List<Country> countries) {
    return Optional.ofNullable(possibilities)
        .flatMap(set -> set.stream().filter(countries::contains).findFirst());
  }

  private static ParsedField<ParsedLocation> fail() {
    return ParsedField.fail(Collections.singletonList(COUNTRY_COORDINATE_MISMATCH.name()));
  }

  private static ParsedField<ParsedLocation> success(Country country, LatLng latLng, List<String> issues) {
    ParsedLocation pl = new ParsedLocation(country, latLng);
    return ParsedField.success(pl, issues);
  }

  private static ParsedField<ParsedLocation> success(Country country, LatLng latLng, OccurrenceIssue issue) {
    return success(country, latLng, Collections.singletonList(issue.name()));
  }

  private static ParsedField<ParsedLocation> success(Country country, LatLng latLng) {
    ParsedLocation pl = new ParsedLocation(country, latLng);
    return ParsedField.success(pl);
  }

  /**
   * Checks if the country and latitude belongs to Antarctica. Rule: country must be
   * Country.ANTARCTICA or null and latitude must be less than (south of) {@link
   * #ANTARCTICA_LATITUDE} but not less than -90°.
   */
  private static boolean isAntarctica(Double latitude, Country country) {
    return latitude != null
        && (country == null || country == Country.ANTARCTICA)
        && (latitude >= -90d && latitude < ANTARCTICA_LATITUDE);
  }
}
