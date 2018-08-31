package org.gbif.pipelines.parsers.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.ws.BaseMockServerTest;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH;
import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES;

public class LocationMatcherTest extends BaseMockServerTest {

  @Test
  public void countryAndCoordsMatchIdentityTest() throws IOException {

    // State
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(coordsCanada, canada, getWsConfig()).applyMatch();

    // Should
    Assert.assertEquals(canada, result.getResult().getCountry());
    Assert.assertEquals(coordsCanada, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void countryAndCoordsMatchIdentityAdditionalMatcherTest() throws IOException {

    // State
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(coordsCanada, canada, getWsConfig())
            .addAdditionalTransform(CoordinatesFunction.NEGATED_LAT_FN)
            .addAdditionalTransform(CoordinatesFunction.NEGATED_LNG_FN)
            .addAdditionalTransform(CoordinatesFunction.NEGATED_COORDS_FN)
            .addAdditionalTransform(CoordinatesFunction.SWAPPED_COORDS_FN)
            .applyMatch();

    // Should
    Assert.assertEquals(canada, result.getResult().getCountry());
    Assert.assertEquals(coordsCanada, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void coordsIdentityCountryFoundTest() throws IOException {

    // State
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(coordsCanada, null, getWsConfig()).applyMatch();

    // Should
    Assert.assertEquals(Country.CANADA, result.getResult().getCountry());
    Assert.assertEquals(coordsCanada, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(result.getIssues().get(0), COUNTRY_DERIVED_FROM_COORDINATES.name());
  }

  @Test
  public void wrongCoordsWhenMatchWithAlternativesCountryNotFoundTest() {

    // State
    enqueueEmptyResponse();

    LatLng wrongCoords = new LatLng(-50, 100);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(wrongCoords, null, getWsConfig())
            .addAdditionalTransform(CoordinatesFunction.NEGATED_LAT_FN)
            .addAdditionalTransform(CoordinatesFunction.NEGATED_LNG_FN)
            .addAdditionalTransform(CoordinatesFunction.NEGATED_COORDS_FN)
            .addAdditionalTransform(CoordinatesFunction.SWAPPED_COORDS_FN)
            .applyMatch();

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(result.getIssues().get(0), COUNTRY_COORDINATE_MISMATCH.name());
  }

  @Test
  public void coordsAntarcticaFoundEmptyTest() {

    // State
    enqueueEmptyResponse();

    LatLng antarcticaEdgeCoords = new LatLng(-61, -130);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(antarcticaEdgeCoords, null, getWsConfig()).applyMatch();

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(Country.ANTARCTICA, result.getResult().getCountry());
  }

  @Test
  public void countryAndNegatedIdentityFailTest() {

    // State
    enqueueEmptyResponse();

    Country canada = Country.CANADA;
    LatLng negatedLatCoords = new LatLng(-LATITUDE_CANADA, LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(negatedLatCoords, canada, getWsConfig()).applyMatch();

    // Should
    Assert.assertFalse(result.isSuccessful());
  }

  @Test
  public void countryAndNegatedLatTest() throws IOException {

    // State
    enqueueEmptyResponse();
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng negatedLatCoords = new LatLng(-LATITUDE_CANADA, LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(negatedLatCoords, canada, getWsConfig())
            .addAdditionalTransform(CoordinatesFunction.NEGATED_LAT_FN)
            .applyMatch();

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
  public void countryAndNegatedLngTest() throws IOException {

    // State
    enqueueEmptyResponse();
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng negatedLngCoords = new LatLng(LATITUDE_CANADA, -LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(negatedLngCoords, canada, getWsConfig())
            .addAdditionalTransform(CoordinatesFunction.NEGATED_LNG_FN)
            .applyMatch();

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
  public void countryAndNegatedCoordsTest() throws IOException {

    // State
    enqueueEmptyResponse();
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng negatedCoords = new LatLng(-LATITUDE_CANADA, -LONGITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(negatedCoords, canada, getWsConfig())
            .addAdditionalTransform(CoordinatesFunction.NEGATED_COORDS_FN)
            .applyMatch();

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
  public void countryAndSwappedTest() throws IOException {

    // State
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng swappedCoords = new LatLng(LONGITUDE_CANADA, LATITUDE_CANADA);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(swappedCoords, canada, getWsConfig())
            .addAdditionalTransform(CoordinatesFunction.SWAPPED_COORDS_FN)
            .applyMatch();

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
  public void matchReturnEquivalentTest() throws IOException {

    // State
    enqueueResponse(MOROCCO_WESTERN_SAHARA_REVERSE_RESPONSE);

    LatLng coords = new LatLng(27.15, -13.20);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(coords, Country.WESTERN_SAHARA, getWsConfig()).applyMatch();

    // Should
    Assert.assertEquals(Country.MOROCCO, result.getResult().getCountry());
    Assert.assertEquals(coords, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void matchConfusedEquivalentTest() throws IOException {

    // State
    enqueueResponse(FRENCH_POLYNESIA_REVERSE_RESPONSE);

    LatLng coords = new LatLng(-17.65, -149.46);

    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(coords, Country.FRANCE, getWsConfig()).applyMatch();

    // Should
    Assert.assertEquals(Country.FRENCH_POLYNESIA, result.getResult().getCountry());
    Assert.assertEquals(coords, result.getResult().getLatLng());
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void matchConfusedReturnConfusedTest() throws IOException {

    // State
    enqueueResponse(GREENLAND_REVERSE_RESPONSE);

    LatLng coords = new LatLng(71.7, -42.6);

    // When
    ParsedField<ParsedLocation> match =
        LocationMatcher.newMatcher(coords, Country.DENMARK, getWsConfig()).applyMatch();

    // Should
    Assert.assertEquals(Country.GREENLAND, match.getResult().getCountry());
    Assert.assertEquals(coords, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertEquals(match.getIssues().get(0), COUNTRY_DERIVED_FROM_COORDINATES.name());
  }

  @Test(expected = NullPointerException.class)
  public void nullValuesTest() {
    // When
    LocationMatcher.newMatcher(null, null, getWsConfig()).applyMatch();
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyCoordinatesTest() {
    // When
    LocationMatcher.newMatcher(new LatLng(), null, getWsConfig()).applyMatch();
  }

  @Test
  public void outOfRangeCoordinatesTest() {
    // When
    ParsedField<ParsedLocation> result =
        LocationMatcher.newMatcher(new LatLng(200, 200), null, getWsConfig()).applyMatch();

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(result.getIssues().get(0), COUNTRY_COORDINATE_MISMATCH.name());
  }
}
