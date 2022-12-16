package org.gbif.pipelines.core.parsers.location;

import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES;

import java.util.Arrays;
import java.util.Collections;
import org.gbif.api.vocabulary.Country;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.parser.CoordinatesFunction;
import org.gbif.pipelines.core.parsers.location.parser.LocationMatcher;
import org.gbif.pipelines.core.parsers.location.parser.ParsedLocation;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;
import org.junit.Assert;
import org.junit.Test;

public class LocationMatcherTest {

  private static final Double LATITUDE_CANADA = 60.4;
  private static final Double LONGITUDE_CANADA = -131.3;
  private static final KeyValueStore<LatLng, GeocodeResponse> GEOCODE_KV_STORE;

  static {
    KeyValueTestStore store = new KeyValueTestStore();
    store.put(LatLng.create(60.4d, -131.3d), toGeocodeResponse(Country.CANADA));
    store.put(LatLng.create(30.2d, 100.2344349d), toGeocodeResponse(Country.CHINA));
    store.put(LatLng.create(30.2d, 100.234435d), toGeocodeResponse(Country.CHINA));
    store.put(LatLng.create(71.7d, -42.6d), toGeocodeResponse(Country.GREENLAND));
    store.put(LatLng.create(-17.65, -149.46), toGeocodeResponse(Country.FRENCH_POLYNESIA));
    store.put(LatLng.create(27.15, -13.20), toGeocodeResponse(Country.MOROCCO));
    store.put(
        LatLng.create(8.6, -82.9, 30_000d), toGeocodeResponse(Country.COSTA_RICA, Country.PANAMA));
    store.put(LatLng.create(-61d, -130d), toGeocodeNonISOResponse("Southern Ocean"));
    GEOCODE_KV_STORE = GeocodeKvStore.create(store);
  }

  private static GeocodeResponse toGeocodeResponse(Country country) {
    Location location = new Location();
    location.setType("Political");
    location.setDistance(0.0d);
    location.setIsoCountryCode2Digit(country.getIso2LetterCode());
    return new GeocodeResponse(Collections.singletonList(location));
  }

  private static GeocodeResponse toGeocodeResponse(Country country1, Country country2) {
    Location location1 = new Location();
    location1.setType("Political");
    location1.setDistance(0.0d);
    location1.setIsoCountryCode2Digit(country1.getIso2LetterCode());

    Location location2 = new Location();
    location2.setType("Political");
    location2.setDistance(0.2d);
    location2.setIsoCountryCode2Digit(country2.getIso2LetterCode());

    return new GeocodeResponse(Arrays.asList(location1, location2));
  }

  private static GeocodeResponse toGeocodeNonISOResponse(String title) {
    Location location = new Location();
    location.setName(title);
    return new GeocodeResponse(Collections.singletonList(location));
  }

  @Test
  public void countryAndCoordsMatchIdentityTest() {

    // State
    Country canada = Country.CANADA;
    LatLng coordsCanada = LatLng.create(LATITUDE_CANADA, LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(coordsCanada, canada, GEOCODE_KV_STORE).apply();

    // Should
    Assert.assertEquals(canada, result.getResult().getCountry());
    Assert.assertEquals(coordsCanada, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void countryAndCoordsMatchLargeUncertaintyTest() {

    // State
    Country panama = Country.PANAMA;
    LatLng coordsPanama = LatLng.create(8.60, -82.9, 30_000d);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(coordsPanama, panama, GEOCODE_KV_STORE).apply();

    // Should
    Assert.assertEquals(panama, result.getResult().getCountry());
    Assert.assertEquals(coordsPanama, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void countryAndCoordsMatchIdentityAdditionalMatcherTest() {

    // State
    Country canada = Country.CANADA;
    LatLng coordsCanada = LatLng.create(LATITUDE_CANADA, LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(coordsCanada, canada, GEOCODE_KV_STORE)
            .additionalTransform(CoordinatesFunction.NEGATED_LAT_FN)
            .additionalTransform(CoordinatesFunction.NEGATED_LNG_FN)
            .additionalTransform(CoordinatesFunction.NEGATED_COORDS_FN)
            .additionalTransform(CoordinatesFunction.SWAPPED_COORDS_FN)
            .apply();

    // Should
    Assert.assertEquals(canada, result.getResult().getCountry());
    Assert.assertEquals(coordsCanada, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void coordsIdentityCountryFoundTest() {

    // State
    LatLng coordsCanada = LatLng.create(LATITUDE_CANADA, LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(coordsCanada, null, GEOCODE_KV_STORE).apply();

    // Should
    Assert.assertEquals(Country.CANADA, result.getResult().getCountry());
    Assert.assertEquals(coordsCanada, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getIssues().contains(COUNTRY_DERIVED_FROM_COORDINATES.name()));
  }

  @Test
  public void wrongCoordsWhenMatchWithAlternativesCountryNotFoundTest() {

    // State
    LatLng wrongCoords = LatLng.create(-50d, 100d);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(wrongCoords, null, GEOCODE_KV_STORE)
            .additionalTransform(CoordinatesFunction.NEGATED_LAT_FN)
            .additionalTransform(CoordinatesFunction.NEGATED_LNG_FN)
            .additionalTransform(CoordinatesFunction.NEGATED_COORDS_FN)
            .additionalTransform(CoordinatesFunction.SWAPPED_COORDS_FN)
            .apply();

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void coordsAntarcticaFoundEmptyTest() {

    // State
    LatLng antarcticaEdgeCoords = LatLng.create(-61d, -130d);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(antarcticaEdgeCoords, null, GEOCODE_KV_STORE).apply();

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(Country.ANTARCTICA, result.getResult().getCountry());
  }

  @Test
  public void countryAndNegatedIdentityFailTest() {

    // State
    Country canada = Country.CANADA;
    LatLng negatedLatCoords = LatLng.create(-LATITUDE_CANADA, LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(negatedLatCoords, canada, GEOCODE_KV_STORE).apply();

    // Should
    Assert.assertFalse(result.isSuccessful());
  }

  @Test
  public void countryAndNegatedLatTest() {

    // State

    Country canada = Country.CANADA;
    LatLng coordsCanada = LatLng.create(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng negatedLatCoords = LatLng.create(-LATITUDE_CANADA, LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(negatedLatCoords, canada, GEOCODE_KV_STORE)
            .additionalTransform(CoordinatesFunction.NEGATED_LAT_FN)
            .apply();

    // Should
    Assert.assertEquals(canada, result.getResult().getCountry());
    Assert.assertEquals(coordsCanada, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(
        result
            .getIssues()
            .containsAll(CoordinatesFunction.getIssueTypes(CoordinatesFunction.NEGATED_LAT_FN)));
  }

  @Test
  public void countryAndNegatedLngTest() {

    // State
    Country canada = Country.CANADA;
    LatLng coordsCanada = LatLng.create(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng negatedLngCoords = LatLng.create(LATITUDE_CANADA, -LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(negatedLngCoords, canada, GEOCODE_KV_STORE)
            .additionalTransform(CoordinatesFunction.NEGATED_LNG_FN)
            .apply();

    // Should
    Assert.assertEquals(canada, result.getResult().getCountry());
    Assert.assertEquals(coordsCanada, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(
        result
            .getIssues()
            .containsAll(CoordinatesFunction.getIssueTypes(CoordinatesFunction.NEGATED_LNG_FN)));
  }

  @Test
  public void countryAndNegatedCoordsTest() {

    // State
    Country canada = Country.CANADA;
    LatLng coordsCanada = LatLng.create(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng negatedCoords = LatLng.create(-LATITUDE_CANADA, -LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(negatedCoords, canada, GEOCODE_KV_STORE)
            .additionalTransform(CoordinatesFunction.NEGATED_COORDS_FN)
            .apply();

    // Should
    Assert.assertEquals(canada, result.getResult().getCountry());
    Assert.assertEquals(coordsCanada, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(
        result
            .getIssues()
            .containsAll(CoordinatesFunction.getIssueTypes(CoordinatesFunction.NEGATED_COORDS_FN)));
  }

  @Test
  public void countryAndSwappedTest() {

    // State
    Country canada = Country.CANADA;
    LatLng coordsCanada = LatLng.create(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng swappedCoords = LatLng.create(LONGITUDE_CANADA, LATITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(swappedCoords, canada, GEOCODE_KV_STORE)
            .additionalTransform(CoordinatesFunction.SWAPPED_COORDS_FN)
            .apply();

    // Should
    Assert.assertEquals(canada, result.getResult().getCountry());
    Assert.assertEquals(coordsCanada, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(
        result
            .getIssues()
            .containsAll(CoordinatesFunction.getIssueTypes(CoordinatesFunction.SWAPPED_COORDS_FN)));
  }

  @Test
  public void matchReturnEquivalentTest() {

    // State
    LatLng coords = LatLng.create(27.15, -13.20);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(coords, Country.WESTERN_SAHARA, GEOCODE_KV_STORE).apply();

    // Should
    Assert.assertEquals(Country.MOROCCO, result.getResult().getCountry());
    Assert.assertEquals(coords, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void matchConfusedEquivalentTest() {

    // State
    LatLng coords = LatLng.create(-17.65, -149.46);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(coords, Country.FRANCE, GEOCODE_KV_STORE).apply();

    // Should
    Assert.assertEquals(Country.FRENCH_POLYNESIA, result.getResult().getCountry());
    Assert.assertEquals(coords, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void matchConfusedReturnConfusedTest() {

    // State
    LatLng coords = LatLng.create(71.7d, -42.6d);

    // When
    ParsedField<ParsedLocation> match =
        LocationMatcher.create(coords, Country.DENMARK, GEOCODE_KV_STORE).apply();

    // Should
    Assert.assertEquals(Country.GREENLAND, match.getResult().getCountry());
    Assert.assertEquals(coords, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues().contains(COUNTRY_DERIVED_FROM_COORDINATES.name()));
  }

  @Test(expected = NullPointerException.class)
  public void nullValuesTest() {
    // When
    LocationMatcher.create(null, null, GEOCODE_KV_STORE).apply();
  }

  @Test
  public void outOfRangeCoordinatesTest() {
    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.create(LatLng.create(200d, 200d), null, GEOCODE_KV_STORE).apply();

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }
}
