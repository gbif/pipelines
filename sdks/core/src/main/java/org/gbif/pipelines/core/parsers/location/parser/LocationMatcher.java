package org.gbif.pipelines.core.parsers.location.parser;

import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH;
import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.rest.client.geocode.GeocodeResponse;

/** Matches the location fields related to Country and Coordinates to find possible mismatches. */
@Slf4j
@AllArgsConstructor(staticName = "create")
public class LocationMatcher {

  // Antarctica: "Territories south of 60° south latitude"
  private static final double ANTARCTICA_LATITUDE = -60d;

  private static final Set<String> LAYERS_FILTER_SET =
      new HashSet<>(Arrays.asList("Political", "EEZ", "PoliticalEEZ"));

  private final GeocodeRequest latLng;
  private final Country country;
  private final KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore;
  private final List<UnaryOperator<GeocodeRequest>> alternativeTransformations = new ArrayList<>();

  public LocationMatcher additionalTransform(UnaryOperator<GeocodeRequest> transformation) {
    alternativeTransformations.add(transformation);
    return this;
  }

  public ParsedField<ParsedLocation> apply() {
    // Check parameters
    Objects.requireNonNull(latLng);
    if (latLng.getLat() == null || latLng.getLng() == null) {
      throw new IllegalArgumentException("Empty coordinates");
    }

    // Match country
    return country != null ? applyWithCountry() : applyWithoutCountry();
  }

  private ParsedField<ParsedLocation> applyWithCountry() {

    Optional<List<Country>> countriesKv = getCountryFromCoordinates(latLng);

    // if the WS returned countries we try to match with them
    if (countriesKv.isPresent()) {
      List<Country> countries = countriesKv.get();
      if (countries.contains(this.country)) {
        // country found
        return success(this.country, latLng);
      }

      // if not found, try with equivalent countries
      Optional<Country> equivalentMatch =
          containsAnyCountry(CountryMaps.equivalent(this.country), countries);
      if (equivalentMatch.isPresent()) {
        // country found
        return success(equivalentMatch.get(), latLng);
      }

      // if not found, try with confused countries
      Optional<Country> confusedMatch =
          containsAnyCountry(CountryMaps.confused(this.country), countries);
      if (confusedMatch.isPresent()) {
        // country found
        return success(confusedMatch.get(), latLng, COUNTRY_DERIVED_FROM_COORDINATES);
      }
    }

    // if still not found, try alternatives
    for (UnaryOperator<GeocodeRequest> transformation : alternativeTransformations) {
      // transform location
      GeocodeRequest latLngTransformed = transformation.apply(latLng);

      // call ws
      Optional<List<Country>> countriesFound = getCountryFromCoordinates(latLngTransformed);
      if (countriesFound.filter(x -> x.contains(country)).isPresent()) {
        // country found
        // Add issues from the transformation
        return success(
            country, latLngTransformed, CoordinatesFunction.getIssueTypes(transformation));
      }
    }

    // no result found
    return ParsedField.fail(Collections.singleton(COUNTRY_COORDINATE_MISMATCH.name()));
  }

  private ParsedField<ParsedLocation> applyWithoutCountry() {
    // call WS with identity coords
    return getCountryFromCoordinates(latLng)
        .filter(v -> !v.isEmpty())
        .map(v -> v.iterator().next())
        .map(v -> success(v, latLng, COUNTRY_DERIVED_FROM_COORDINATES))
        .orElse(ParsedField.fail());
  }

  private Optional<List<Country>> getCountryFromCoordinates(GeocodeRequest latLng) {
    if (latLng.isValid()) {
      if (isAntarctica(latLng.getLat(), this.country)) {
        return Optional.of(Collections.singletonList(Country.ANTARCTICA));
      }

      GeocodeResponse geocodeResponse = geocodeKvStore.get(latLng);
      if (geocodeResponse != null && !geocodeResponse.getLocations().isEmpty()) {
        return Optional.of(
            geocodeResponse.getLocations().stream()
                .filter(l -> LAYERS_FILTER_SET.contains(l.getType()))
                .sorted(Comparator.comparingDouble(GeocodeResponse.Location::getDistance))
                .map(GeocodeResponse.Location::getIsoCountryCode2Digit)
                .map(Country::fromIsoCode)
                .collect(Collectors.toList()));
      }
    }
    return Optional.empty();
  }

  private static Optional<Country> containsAnyCountry(
      Set<Country> possibilities, List<Country> countries) {
    if (possibilities == null
        || possibilities.isEmpty()
        || countries == null
        || countries.isEmpty()) {
      return Optional.empty();
    }

    return countries.stream().filter(possibilities::contains).findFirst();
  }

  private static ParsedField<ParsedLocation> success(
      Country country, GeocodeRequest latLng, Set<String> issues) {
    ParsedLocation pl = new ParsedLocation(country, latLng);
    return ParsedField.success(pl, issues);
  }

  private static ParsedField<ParsedLocation> success(
      Country country, GeocodeRequest latLng, OccurrenceIssue issue) {
    return success(country, latLng, Collections.singleton(issue.name()));
  }

  private static ParsedField<ParsedLocation> success(Country country, GeocodeRequest latLng) {
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
