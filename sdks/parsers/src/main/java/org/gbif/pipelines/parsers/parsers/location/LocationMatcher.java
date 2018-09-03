package org.gbif.pipelines.parsers.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.parsers.legacy.CountryMaps;
import org.gbif.pipelines.parsers.ws.HttpResponse;
import org.gbif.pipelines.parsers.ws.client.geocode.GeocodeServiceClient;
import org.gbif.pipelines.parsers.ws.config.WsConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH;
import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES;

/** Matches the location fields related to Country and Coordinates to find possible mismatches. */
class LocationMatcher {

  private static final Logger LOG = LoggerFactory.getLogger(LocationMatcher.class);

  // Antarctica: "Territories south of 60° south latitude"
  private static final double ANTARCTICA_LATITUDE = -60d;
  private static final Predicate<List> CHECK_LIST = list -> list != null && !list.isEmpty();

  private final LatLng latLng;
  private final Country country;
  private final GeocodeServiceClient geocodeServiceClient;
  private final List<Function<LatLng, LatLng>> alternativeTransformations = new ArrayList<>();

  private LocationMatcher(LatLng latLng, Country country, WsConfig wsConfig) {
    this.latLng = latLng;
    this.country = country;
    this.geocodeServiceClient = GeocodeServiceClient.create(wsConfig);
  }

  static LocationMatcher create(LatLng latLng, Country country, WsConfig wsConfig) {
    return new LocationMatcher(latLng, country, wsConfig);
  }

  LocationMatcher additionalTransform(Function<LatLng, LatLng> transormation) {
    alternativeTransformations.add(transormation);
    return this;
  }

  ParsedField<ParsedLocation> apply() {
    // Check parameters
    Objects.requireNonNull(latLng);
    if (latLng.getLat() == null || latLng.getLng() == null) {
      throw new IllegalArgumentException("Empty coordinates");
    }

    // Match country
    return country != null ? applyWithCountry() : applyWithoutCountry();
  }

  private ParsedField<ParsedLocation> applyWithCountry() {
    // call WS with identity coords
    List<Country> countries = getCountriesFromCoordinates(latLng);

    // if the WS returned countries we try to match with them
    if (CHECK_LIST.test(countries)) {
      if (countries.contains(country)) {
        // country found
        return success(country, latLng);
      }

      // if not found, try with equivalent countries
      Optional<Country> equivalentMatch =
          containsAnyCountry(CountryMaps.equivalent(country), countries);
      if (equivalentMatch.isPresent()) {
        // country found
        return success(equivalentMatch.get(), latLng);
      }

      // if not found, try with confused countries
      Optional<Country> confusedMatch =
          containsAnyCountry(CountryMaps.confused(country), countries);
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
      List<Country> countriesFound = getCountriesFromCoordinates(latLngTransformed);
      if (CHECK_LIST.test(countriesFound) && countriesFound.contains(country)) {
        // country found
        // Add issues from the transformation
        return success(
            country, latLngTransformed, CoordinatesFunction.getIssueTypes(transformation));
      }
    }

    // no result found
    return fail();
  }

  private ParsedField<ParsedLocation> applyWithoutCountry() {
    // call WS with identity coords
    List<Country> countries = getCountriesFromCoordinates(latLng);

    if (!CHECK_LIST.test(countries)) {
      return fail();
    }
    return success(countries.get(0), latLng, COUNTRY_DERIVED_FROM_COORDINATES);
  }

  private List<Country> getCountriesFromCoordinates(LatLng latLng) {
    HttpResponse<List<Country>> response = geocodeServiceClient.getCountriesFromLatLng(latLng);

    if (response.isError()) {
      LOG.info("Error calling the geocode WS: {}", response.getErrorMessage());
      return Collections.emptyList();
    }

    if ((response.getBody() == null || response.getBody().isEmpty())
        && isAntarctica(latLng.getLat(), country)) {
      return Collections.singletonList(Country.ANTARCTICA);
    }

    return response.getBody();
  }

  private static Optional<Country> containsAnyCountry(
      Set<Country> possibilities, List<Country> countries) {
    return Optional.ofNullable(possibilities)
        .flatMap(set -> set.stream().filter(countries::contains).findFirst());
  }

  private static ParsedField<ParsedLocation> fail() {
    return ParsedField.fail(Collections.singletonList(COUNTRY_COORDINATE_MISMATCH.name()));
  }

  private static ParsedField<ParsedLocation> success(
      Country country, LatLng latLng, List<String> issues) {
    ParsedLocation pl = new ParsedLocation(country, latLng);
    return ParsedField.success(pl, issues);
  }

  private static ParsedField<ParsedLocation> success(
      Country country, LatLng latLng, OccurrenceIssue issue) {
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
