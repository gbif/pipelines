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
    return country != null ? applyMatchWithCountry() : applyMatchWithoutCountry();
  }

  private ParsedField<ParsedLocation> applyMatchWithCountry() {
    // call WS with identity coords
    List<Country> countries = getCountriesFromCoordinates(latLng);

    // if the WS returned countries we try to match with them
    if (countries != null && !countries.isEmpty()) {
      if (countries.contains(country)) {
        // country found
        return ParsedField.success(ParsedLocation.newBuilder().country(country).latLng(latLng).build());
      }

      // if not found, try with equivalent countries
      Optional<Country> equivalentMatch = containsAnyCountry(CountryMaps.equivalent(country), countries);
      if (equivalentMatch.isPresent()) {
        // country found
        return ParsedField.success(ParsedLocation.newBuilder().country(equivalentMatch.get()).latLng(latLng).build());
      }

      // if not found, try with confused countries
      Optional<Country> confusedMatch = containsAnyCountry(CountryMaps.confused(country), countries);
      if (confusedMatch.isPresent()) {
        // country found
        return ParsedField.success(ParsedLocation.newBuilder().country(confusedMatch.get()).latLng(latLng).build(),
                                   Collections.singletonList(new InterpretationIssue(IssueType.COUNTRY_DERIVED_FROM_COORDINATES,
                                                                                     DwcTerm.country)));
      }
    }

    // if still not found, try alternatives
    LOG.info("Trying alternative transformations to the coordinates");
    for (CoordinatesFunction transformation : alternativeTransformations) {
      // transform location
      LatLng latLngTransformed = transformation.getTransformer().apply(latLng);

      // call ws
      List<Country> countriesFound = getCountriesFromCoordinates(latLngTransformed);
      if (countriesFound == null || countriesFound.isEmpty()) {
        continue;
      }

      if (countriesFound.contains(country)) {
        // country found
        // Add issues from the transformation
        List<InterpretationIssue> issues = new ArrayList<>();
        CoordinatesFunction.getIssueTypes(transformation)
          .forEach(issueType -> issues.add(new InterpretationIssue(issueType, getCountryAndCoordinatesTerms())));
        // return success with the issues
        return ParsedField.success(ParsedLocation.newBuilder().country(country).latLng(latLngTransformed).build(),
                                   issues);
      }
    }

    // no result found
    return getFailResponse();
  }

  private ParsedField<ParsedLocation> applyMatchWithoutCountry() {
    // call WS with identity coords
    List<Country> countries = getCountriesFromCoordinates(latLng);

    if (countries == null || countries.isEmpty()) {
      // country not found
      return getFailResponse();
    }

    // country found
    return ParsedField.success(ParsedLocation.newBuilder().country(countries.get(0)).latLng(latLng).build(),
                               Collections.singletonList(new InterpretationIssue(IssueType.COUNTRY_DERIVED_FROM_COORDINATES,
                                                                                 DwcTerm.country)));
  }

  private List<Country> getCountriesFromCoordinates(LatLng latLng) {
    HttpResponse<List<Country>> response = GeocodeServiceClient.getInstance().getCountriesFromLatLng(latLng);

    if (response.isError()) {
      LOG.info("Error calling the geocode WS: {}", response.getErrorMessage());
      return Collections.emptyList();
    }

    if ((response.getBody() == null || response.getBody().isEmpty()) && isAntarctica(latLng.getLat(), country)) {
      return Collections.singletonList(Country.ANTARCTICA);
    }

    return response.getBody();
  }

  private static Optional<Country> containsAnyCountry(
    Collection<Country> possibilities, Collection<Country> countries
  ) {
    return possibilities != null ? possibilities.stream().filter(countries::contains).findFirst() : Optional.empty();
  }

  private static ParsedField<ParsedLocation> getFailResponse() {
    return ParsedField.fail(Collections.singletonList(new InterpretationIssue(IssueType.COUNTRY_COORDINATE_MISMATCH,
                                                                              getCountryAndCoordinatesTerms())));
  }

  private static List<Term> getCountryAndCoordinatesTerms() {
    return Arrays.asList(DwcTerm.country, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude);
  }
}