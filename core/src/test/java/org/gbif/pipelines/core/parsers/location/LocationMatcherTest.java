package org.gbif.pipelines.core.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.pipelines.core.parsers.ParsedField;
import org.gbif.pipelines.core.ws.MockServer;
import org.gbif.pipelines.io.avro.IssueType;

import java.io.IOException;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class LocationMatcherTest extends MockServer {

  @BeforeClass
  public static void setUp() throws IOException {
    mockServerSetUp();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    mockServerTearDown();
  }

  @Test
  public void givenCountryAndCoordsWhenMatchIdentityThenSuccess() throws IOException {
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);

    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(coordsCanada, canada).applyMatch();

    Assert.assertEquals(canada, match.getResult().getCountry());
    Assert.assertEquals(coordsCanada, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues().isEmpty());

    enqueueResponse(CANADA_REVERSE_RESPONSE);

    // should not execute any additional transformations
    match = LocationMatcher.newMatcher(coordsCanada, canada)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_LAT)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_LNG)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_COORDS)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_SWAPPED_COORDS)
      .applyMatch();

    Assert.assertEquals(canada, match.getResult().getCountry());
    Assert.assertEquals(coordsCanada, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues().isEmpty());
  }

  @Test
  public void givenCoordsWhenMatchIdentityThenCountryFound() throws IOException {
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);

    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(coordsCanada).applyMatch();

    Country canada = Country.CANADA;
    Assert.assertEquals(canada, match.getResult().getCountry());
    Assert.assertEquals(coordsCanada, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues().get(0).getIssueType().equals(IssueType.COUNTRY_DERIVED_FROM_COORDINATES));
  }

  @Test
  public void givenWrongCoordsWhenMatchWithAlternativesThenCountryNotFound() throws IOException {
    enqueueEmptyResponse();

    LatLng wrongCoords = new LatLng(-50, 100);

    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(wrongCoords)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_LAT)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_LNG)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_COORDS)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_SWAPPED_COORDS)
      .applyMatch();

    Assert.assertFalse(match.isSuccessful());
    Assert.assertTrue(match.getIssues().get(0).getIssueType().equals(IssueType.COUNTRY_COORDINATE_MISMATCH));
  }

  @Test
  public void givenAntarcticaCoordsWhenMatchThenAntarcticaFound() throws IOException {
    enqueueEmptyResponse();

    LatLng antarcticaEdgeCoords = new LatLng(-61, -130);

    // in this case the ws returns empty response
    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(antarcticaEdgeCoords).applyMatch();

    Country antarctica = Country.ANTARCTICA;
    Assert.assertTrue(match.isSuccessful());
    Assert.assertEquals(antarctica, match.getResult().getCountry());

    enqueueResponse(ANTARCTICA_REVERSE_RESPONSE);

    LatLng antarcticaCoords = new LatLng(-81, -130);

    // in this case, the ws returns antarctica
    match = LocationMatcher.newMatcher(antarcticaEdgeCoords).applyMatch();
    Assert.assertTrue(match.isSuccessful());
    Assert.assertEquals(antarctica, match.getResult().getCountry());
  }

  @Test
  public void givenCountryAndNegatedCoordsWhenMatchIdentityThenFail() throws IOException {
    enqueueEmptyResponse();

    Country canada = Country.CANADA;
    LatLng negatedLatCoords = new LatLng(-LATITUDE_CANADA, LONGITUDE_CANADA);

    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(negatedLatCoords, canada).applyMatch();

    Assert.assertFalse(match.isSuccessful());
  }

  @Test
  public void givenCountryAndNegatedLatWhenMatchWithAdditionalTransformThenSuccess() throws IOException {
    enqueueEmptyResponse();
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng negatedLatCoords = new LatLng(-LATITUDE_CANADA, LONGITUDE_CANADA);

    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(negatedLatCoords, canada)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_LAT)
      .applyMatch();

    Assert.assertEquals(canada, match.getResult().getCountry());
    Assert.assertEquals(coordsCanada, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues()
                        .stream()
                        .map(issue -> issue.getIssueType())
                        .collect(Collectors.toList())
                        .containsAll(CoordinatesTransformation.getIssueTypes(CoordinatesTransformation.PRESUMED_NEGATED_LAT)));
  }

  @Test
  public void givenCountryAndNegatedLngWhenMatchWithAdditionalTransformThenSuccess() throws IOException {
    enqueueResponse(RUSSIA_REVERSE_RESPONSE);
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng negatedLngCoords = new LatLng(LATITUDE_CANADA, -LONGITUDE_CANADA);

    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(negatedLngCoords, canada)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_LNG)
      .applyMatch();

    Assert.assertEquals(canada, match.getResult().getCountry());
    Assert.assertEquals(coordsCanada, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues()
                        .stream()
                        .map(issue -> issue.getIssueType())
                        .collect(Collectors.toList())
                        .containsAll(CoordinatesTransformation.getIssueTypes(CoordinatesTransformation.PRESUMED_NEGATED_LNG)));
  }

  @Test
  public void givenCountryAndNegatedCoordsWhenMatchWithAdditionalTransformThenSuccess() throws IOException {
    enqueueEmptyResponse();
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng negatedCoords = new LatLng(-LATITUDE_CANADA, -LONGITUDE_CANADA);

    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(negatedCoords, canada)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_COORDS)
      .applyMatch();

    Assert.assertEquals(canada, match.getResult().getCountry());
    Assert.assertEquals(coordsCanada, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues()
                        .stream()
                        .map(issue -> issue.getIssueType())
                        .collect(Collectors.toList())
                        .containsAll(CoordinatesTransformation.getIssueTypes(CoordinatesTransformation.PRESUMED_NEGATED_COORDS)));
  }

  @Test
  public void givenCountryAndSwappedCoordsWhenMatchWithAdditionalTransformThenSuccess() throws IOException {
    // only needs to enqueu one response because the first try is out of range and does not call the ws
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    Country canada = Country.CANADA;
    LatLng coordsCanada = new LatLng(LATITUDE_CANADA, LONGITUDE_CANADA);
    LatLng swappedCoords = new LatLng(LONGITUDE_CANADA, LATITUDE_CANADA);

    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(swappedCoords, canada)
      .addAdditionalTransform(CoordinatesTransformation.PRESUMED_SWAPPED_COORDS)
      .applyMatch();

    Assert.assertEquals(canada, match.getResult().getCountry());
    Assert.assertEquals(coordsCanada, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues()
                        .stream()
                        .map(issue -> issue.getIssueType())
                        .collect(Collectors.toList())
                        .containsAll(CoordinatesTransformation.getIssueTypes(CoordinatesTransformation.PRESUMED_SWAPPED_COORDS)));
  }

  @Test
  public void given2MatcheswhenMatchThenReturnEquivalent() throws IOException {
    enqueueResponse(MOROCCO_WESTERN_SAHARA_REVERSE_RESPONSE);

    LatLng coords = new LatLng(27.15, -13.20);
    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(coords, Country.WESTERN_SAHARA).applyMatch();

    Assert.assertEquals(Country.MOROCCO, match.getResult().getCountry());
    Assert.assertEquals(coords, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues().isEmpty());
  }

  @Test
  public void whenMatchConfusedCountryThenReturnEquivalent() throws IOException {
    enqueueResponse(FRENCH_POLYNESIA_REVERSE_RESPONSE);

    LatLng coords = new LatLng(-17.65, -149.46);
    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(coords, Country.FRANCE).applyMatch();

    Assert.assertEquals(Country.FRENCH_POLYNESIA, match.getResult().getCountry());
    Assert.assertEquals(coords, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues().isEmpty());
  }

  @Test
  public void whenMatchConfusedCountryThenReturnConfused() throws IOException {
    enqueueResponse(GREENLAND_REVERSE_RESPONSE);

    LatLng coords = new LatLng(71.7, -42.6);
    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(coords, Country.DENMARK).applyMatch();

    Assert.assertEquals(Country.GREENLAND, match.getResult().getCountry());
    Assert.assertEquals(coords, match.getResult().getLatLng());
    Assert.assertTrue(match.isSuccessful());
    Assert.assertTrue(match.getIssues().get(0).getIssueType().equals(IssueType.COUNTRY_DERIVED_FROM_COORDINATES));
  }

  @Test(expected = NullPointerException.class)
  public void nullValues() {
    LocationMatcher.newMatcher(null).applyMatch();
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyCoordinates() {
    LocationMatcher.newMatcher(new LatLng()).applyMatch();
  }

  @Test
  public void outOfRangeCoordinates() {
    ParsedField<ParsedLocation> match = LocationMatcher.newMatcher(new LatLng(200, 200)).applyMatch();

    Assert.assertFalse(match.isSuccessful());
    Assert.assertTrue(match.getIssues().get(0).getIssueType().equals(IssueType.COUNTRY_COORDINATE_MISMATCH));
  }

}
