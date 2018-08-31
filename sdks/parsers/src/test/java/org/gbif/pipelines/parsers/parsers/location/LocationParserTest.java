package org.gbif.pipelines.parsers.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.utils.ExtendedRecordBuilder;
import org.gbif.pipelines.parsers.ws.BaseMockServerTest;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_ROUNDED;
import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES;
import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84;

public class LocationParserTest extends BaseMockServerTest {

  private static final String TEST_ID = "1";

  @Test
  public void parseByCountryTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).country("Spain").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getWsConfig());

    // Should
    Assert.assertEquals(Country.SPAIN, result.getResult().getCountry());
  }

  @Test
  public void parseByCountryCodeTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).countryCode("ES").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getWsConfig());

    // Should
    Assert.assertEquals(Country.SPAIN, result.getResult().getCountry());
  }

  @Test
  public void parseByCountryAndAcountryCodeTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).country("Spain").countryCode("ES").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getWsConfig());

    // Should
    Assert.assertEquals(Country.SPAIN, result.getResult().getCountry());
    Assert.assertTrue(result.getIssues().contains(COORDINATE_INVALID.name()));
  }

  @Test
  public void invalidCountryIssueTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).country("foo").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getWsConfig());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertNull(result.getResult().getCountry());
    Assert.assertTrue(result.getIssues().contains(COUNTRY_INVALID.name()));
  }

  @Test
  public void invalidCountryCodeIssue() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).countryCode("foo").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getWsConfig());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertNull(result.getResult().getCountry());
    Assert.assertTrue(result.getIssues().contains(COUNTRY_INVALID.name()));
  }

  @Test
  public void coordsWithDerivedCountryTest() throws IOException {

    // State
    enqueueResponse(CHINA_REVERSE_RESPONSE);

    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id(TEST_ID)
            .decimalLatitude("30.2")
            .decimalLongitude("100.2344349")
            .build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getWsConfig());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(Country.CHINA, result.getResult().getCountry());
    Assert.assertEquals(30.2d, result.getResult().getLatLng().getLat(), 0);
    Assert.assertEquals(100.234435d, result.getResult().getLatLng().getLng(), 0);
    Assert.assertTrue(
        result
            .getIssues()
            .containsAll(
                Arrays.asList(
                    COUNTRY_INVALID.name(),
                    COORDINATE_ROUNDED.name(),
                    COUNTRY_DERIVED_FROM_COORDINATES.name(),
                    GEODETIC_DATUM_ASSUMED_WGS84.name())));
  }

  @Test
  public void verbatimLtnLngWithDerivedCountryTest() throws IOException {

    // State
    enqueueResponse(CHINA_REVERSE_RESPONSE);

    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id(TEST_ID)
            .verbatimLatitude("30.2")
            .verbatimLongitude("100.2344349")
            .build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getWsConfig());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(Country.CHINA, result.getResult().getCountry());
    Assert.assertEquals(30.2d, result.getResult().getLatLng().getLat(), 0);
    Assert.assertEquals(100.234435d, result.getResult().getLatLng().getLng(), 0);
    Assert.assertTrue(
        result
            .getIssues()
            .containsAll(
                Arrays.asList(
                    COUNTRY_INVALID.name(),
                    COORDINATE_ROUNDED.name(),
                    COUNTRY_DERIVED_FROM_COORDINATES.name(),
                    GEODETIC_DATUM_ASSUMED_WGS84.name())));
  }

  @Test
  public void verbatimCoordsWithDerivedCountryTest() throws IOException {

    // State
    enqueueResponse(CHINA_REVERSE_RESPONSE);
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).verbatimCoords("30.2, 100.2344349").build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getWsConfig());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(Country.CHINA, result.getResult().getCountry());
    Assert.assertEquals(30.2d, result.getResult().getLatLng().getLat(), 0);
    Assert.assertEquals(100.234435d, result.getResult().getLatLng().getLng(), 0);
    Assert.assertTrue(
        result
            .getIssues()
            .containsAll(
                Arrays.asList(
                    COUNTRY_INVALID.name(),
                    COORDINATE_ROUNDED.name(),
                    COUNTRY_DERIVED_FROM_COORDINATES.name(),
                    GEODETIC_DATUM_ASSUMED_WGS84.name())));
  }

  @Test
  public void coordsAndCountryWhenParsedThenReturnCoordsAndCountry() throws IOException {

    // State
    enqueueResponse(CANADA_REVERSE_RESPONSE);

    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id(TEST_ID)
            .country(Country.CANADA.getTitle())
            .countryCode(Country.CANADA.getIso2LetterCode())
            .decimalLatitude(String.valueOf(LATITUDE_CANADA))
            .decimalLongitude(String.valueOf(LONGITUDE_CANADA))
            .build();

    // When
    ParsedField<ParsedLocation> result = LocationParser.parse(extendedRecord, getWsConfig());

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(Country.CANADA, result.getResult().getCountry());
    Assert.assertEquals(LATITUDE_CANADA, result.getResult().getLatLng().getLat(), 0);
    Assert.assertEquals(LONGITUDE_CANADA, result.getResult().getLatLng().getLng(), 0);
    Assert.assertEquals(1, result.getIssues().size());
    Assert.assertTrue(result.getIssues().contains(GEODETIC_DATUM_ASSUMED_WGS84.name()));
  }

  @Test(expected = NullPointerException.class)
  public void nullArgs() {
    // When
    LocationParser.parse(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidArgs() {
    // When
    LocationParser.parse(new ExtendedRecord(), null);
  }
}
