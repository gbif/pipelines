package org.gbif.pipelines.core.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.parsers.InterpretationIssue;
import org.gbif.pipelines.core.parsers.ParsedField;
import org.gbif.pipelines.core.parsers.legacy.CountryMaps;
import org.gbif.pipelines.core.ws.HttpResponse;
import org.gbif.pipelines.core.ws.client.geocode.GeocodeServiceClient;
import org.gbif.pipelines.io.avro.IssueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.core.parsers.location.CoordinatesValidator.isAntarctica;

/**
 * Matches the location fields related to Country and Coordinates to find possible mismatches.
 */
public class LocationMatcher {

  private static final Logger LOG = LoggerFactory.getLogger(LocationMatcher.class);

  private final LatLng latLng;
  private final Country country;
  private List<CoordinatesFunction> alternativeTransformations = new ArrayList<>();

  private LocationMatcher(LatLng latLng, Country country) {
    this.latLng = latLng;
    this.country = country;
  }

  public static LocationMatcher newMatcher(LatLng latLng, Country country) {
    return new LocationMatcher(latLng, country);
  }

  public static LocationMatcher newMatcher(LatLng latLng) {
    return new LocationMatcher(latLng, null);
  }

  public LocationMatcher addAdditionalTransform(CoordinatesFunction transformType) {
    alternativeTransformations.add(transformType);
    return this;
  }

  public ParsedField<ParsedLocation> applyMatch() {
    Objects.requireNonNull(latLng);
    CoordinatesValidator.checkEmptyCoordinates(latLng);

    List<InterpretationIssue> issues = new ArrayList<>();

    // try identity first
    Optional<Country> countryMatch = tryIdentityMatch(issues);

    if (countryMatch.isPresent()) {
      // country found
      if (country == null) {
        issues.add(new InterpretationIssue(IssueType.COUNTRY_DERIVED_FROM_COORDINATES, DwcTerm.country));
      }
      return ParsedField.success(ParsedLocation.newBuilder().country(countryMatch.get()).latLng(latLng).build(),
                                 issues);
    }

    if (this.country != null) {
      // try alternative transformations if the country was supplied
      LOG.info("Trying alternative transformations to the coordinates");
      for (CoordinatesFunction transformation : alternativeTransformations) {
        // transform location
        LatLng latLngTransformed = transformation.getTransformer().apply(this.latLng);

        Optional<List<Country>> countriesOpt = tryMatch(latLngTransformed);

        if (countriesOpt.filter(countries -> !countries.isEmpty()).isPresent()) {
          countryMatch = matchWithCountry(countriesOpt.get());

          if (countryMatch.isPresent()) {
            // country found
            // add issue from the transformation
            CoordinatesFunction.getIssueTypes(transformation)
              .forEach(issueType -> issues.add(new InterpretationIssue(issueType, getCountryAndCoordinatesTerms())));
            return ParsedField.success(ParsedLocation.newBuilder()
                                         .country(countryMatch.get())
                                         .latLng(latLngTransformed)
                                         .build(), issues);
          }
        }
      }
    }

    // no result found
    issues.add(new InterpretationIssue(IssueType.COUNTRY_COORDINATE_MISMATCH, getCountryAndCoordinatesTerms()));
    return ParsedField.fail(issues);
  }

  private Optional<Country> tryIdentityMatch(List<InterpretationIssue> issues) {
    // call WS with identity coords
    Optional<List<Country>> countriesOpt = tryMatch(latLng);

    if (!countriesOpt.filter(countries -> !countries.isEmpty()).isPresent()) {
      return Optional.empty();
    }

    List<Country> countries = countriesOpt.get();
    // if not country supplied
    if (country == null) {
      return Optional.ofNullable(countries.get(0));
    }

    // else try toi match with the country supplied
    Optional<Country> countryMatched = matchWithCountry(countries);

    if (countryMatched.isPresent()) {
      return countryMatched;
    }

    // if we have not found it yet, we try with equivalent countries
    Optional<Country> countryMatch = containsAnyCountry(CountryMaps.equivalent(country), countries);
    if (countryMatch.isPresent()) {
      return countryMatch;
    }

    // if we have not found it yet, we try with confused countries
    countryMatch = containsAnyCountry(CountryMaps.confused(country), countries);
    if (countryMatch.isPresent()) {
      issues.add(new InterpretationIssue(IssueType.COUNTRY_DERIVED_FROM_COORDINATES, DwcTerm.country));
      return countryMatch;
    }

    return Optional.empty();
  }

  private Optional<List<Country>> tryMatch(LatLng latLng) {
    HttpResponse<List<Country>> response = GeocodeServiceClient.getInstance().getCountriesFromLatLng(latLng);

    if (response.isError()) {
      LOG.info("Error calling the geocode WS: {}", response.getErrorMessage());
      return Optional.empty();
    }

    if ((response.getBody() == null || response.getBody().isEmpty()) && isAntarctica(latLng.getLat(), country)) {
      return Optional.of(Collections.singletonList(Country.ANTARCTICA));
    }

    return Optional.ofNullable(response.getBody());
  }

  private Optional<Country> matchWithCountry(List<Country> countries) {
    return countries.contains(country) ? Optional.ofNullable(country) : Optional.empty();
  }

  private Optional<Country> containsAnyCountry(Collection<Country> possibilities, Collection<Country> countries) {
    if (possibilities != null) {
      return possibilities.stream().filter(countries::contains).findFirst();
    }
    return Optional.empty();
  }

  private List<Term> getCountryAndCoordinatesTerms() {
    return Arrays.asList(DwcTerm.country, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude);
  }

}
