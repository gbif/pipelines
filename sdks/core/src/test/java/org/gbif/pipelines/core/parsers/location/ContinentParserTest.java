package org.gbif.pipelines.core.parsers.location;

import static org.gbif.api.vocabulary.OccurrenceIssue.*;

import java.util.Arrays;
import java.util.Collections;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.parser.ContinentParser;
import org.gbif.pipelines.core.utils.ExtendedRecordBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;
import org.junit.Assert;
import org.junit.Test;

public class ContinentParserTest {

  private static final String TEST_ID = "1";

  private static final KeyValueStore<LatLng, GeocodeResponse> GEOCODE_KV_STORE;

  static {
    KeyValueTestStore testStore = new KeyValueTestStore();
    testStore.put(new LatLng(38.7, 29.6), toGeocodeResponse(Continent.ASIA, Country.TURKEY));
    testStore.put(new LatLng(30.0, -20.0), toGeocodeResponse(Country.SPAIN));

    GEOCODE_KV_STORE = GeocodeKvStore.create(testStore);
  }

  private static GeocodeResponse toGeocodeResponse(Continent continent, Country country) {
    Location location = new Location();
    location.setType("Continent");
    location.setDistance(0.0d);
    location.setId(continent.name());

    Location countryLocation = new Location();
    countryLocation.setType("Political");
    countryLocation.setDistance(0.0d);
    countryLocation.setIsoCountryCode2Digit(country.getIso2LetterCode());
    return new GeocodeResponse(Arrays.asList(location, countryLocation));
  }

  private static GeocodeResponse toGeocodeResponse(Country country) {
    Location location = new Location();
    location.setType("Political");
    location.setDistance(0.0d);
    location.setIsoCountryCode2Digit(country.getIso2LetterCode());
    return new GeocodeResponse(Collections.singletonList(location));
  }

  private KeyValueStore<LatLng, GeocodeResponse> getGeocodeKvStore() {
    return GEOCODE_KV_STORE;
  }

  @Test
  public void parseByContinentTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).country("TURKİYE").continent("EUROPA!").build();
    LocationRecord locationRecord = LocationRecord.newBuilder().setId(TEST_ID).build();

    // When
    ParsedField<Continent> result =
        ContinentParser.parseContinent(extendedRecord, locationRecord, getGeocodeKvStore());

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(Continent.EUROPE, result.getResult());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void invalidContinentIssueTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).continent("Atlantic").build();
    LocationRecord locationRecord = LocationRecord.newBuilder().setId(TEST_ID).build();

    // When
    ParsedField<Continent> result =
        ContinentParser.parseContinent(extendedRecord, locationRecord, getGeocodeKvStore());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertNull(result.getResult());
    Assert.assertTrue(result.getIssues().contains(CONTINENT_INVALID.name()));
    Assert.assertEquals(1, result.getIssues().size());
  }

  @Test
  public void mismatchingContinentIssueTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).continent("Africa").build();
    LocationRecord locationRecord =
        LocationRecord.newBuilder().setId(TEST_ID).setCountryCode("GR").build();

    // When
    ParsedField<Continent> result =
        ContinentParser.parseContinent(extendedRecord, locationRecord, getGeocodeKvStore());

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(Continent.AFRICA, result.getResult());
    Assert.assertTrue(result.getIssues().contains(CONTINENT_COUNTRY_MISMATCH.name()));
    Assert.assertEquals(1, result.getIssues().size());
  }

  @Test
  public void continentFromCountryTest() {

    // State
    ExtendedRecord extendedRecord = ExtendedRecordBuilder.create().id(TEST_ID).build();
    LocationRecord locationRecord =
        LocationRecord.newBuilder().setId(TEST_ID).setCountryCode("CH").build();

    // When
    ParsedField<Continent> result =
        ContinentParser.parseContinent(extendedRecord, locationRecord, getGeocodeKvStore());

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(Continent.EUROPE, result.getResult());
    Assert.assertTrue(result.getIssues().contains(CONTINENT_DERIVED_FROM_COUNTRY.name()));
    Assert.assertEquals(1, result.getIssues().size());
  }

  @Test
  public void continentNotFromCountryTest() {

    // State
    ExtendedRecord extendedRecord = ExtendedRecordBuilder.create().id(TEST_ID).build();
    LocationRecord locationRecord =
        LocationRecord.newBuilder().setId(TEST_ID).setCountryCode("TR").build();

    // When
    ParsedField<Continent> result =
        ContinentParser.parseContinent(extendedRecord, locationRecord, getGeocodeKvStore());

    // Should
    Assert.assertFalse(result.isSuccessful());
    Assert.assertNull(result.getResult());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void wrongContinentAndCoordsTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).continent("Europe").build();
    LocationRecord locationRecord =
        LocationRecord.newBuilder()
            .setId(TEST_ID)
            .setDecimalLatitude(38.7)
            .setDecimalLongitude(29.6)
            .build();

    // When
    ParsedField<Continent> result =
        ContinentParser.parseContinent(extendedRecord, locationRecord, getGeocodeKvStore());

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(Continent.EUROPE, result.getResult());
    Assert.assertTrue(result.getIssues().contains(CONTINENT_COORDINATE_MISMATCH.name()));
    Assert.assertEquals(1, result.getIssues().size());
  }

  @Test
  public void continentAndCoordsTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).continent("Asia").build();
    LocationRecord locationRecord =
        LocationRecord.newBuilder()
            .setId(TEST_ID)
            .setDecimalLatitude(38.7)
            .setDecimalLongitude(29.6)
            .build();

    // When
    ParsedField<Continent> result =
        ContinentParser.parseContinent(extendedRecord, locationRecord, getGeocodeKvStore());

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(Continent.ASIA, result.getResult());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void continentFromCoordsTest() {

    // State
    ExtendedRecord extendedRecord = ExtendedRecordBuilder.create().id(TEST_ID).build();
    LocationRecord locationRecord =
        LocationRecord.newBuilder()
            .setId(TEST_ID)
            .setDecimalLatitude(38.7)
            .setDecimalLongitude(29.6)
            .build();

    // When
    ParsedField<Continent> result =
        ContinentParser.parseContinent(extendedRecord, locationRecord, getGeocodeKvStore());

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(Continent.ASIA, result.getResult());
    Assert.assertTrue(result.getIssues().contains(CONTINENT_DERIVED_FROM_COORDINATES.name()));
    Assert.assertEquals(1, result.getIssues().size());
  }

  @Test
  public void inTheOceanTest() {

    // State
    ExtendedRecord extendedRecord = ExtendedRecordBuilder.create().id(TEST_ID).build();
    LocationRecord locationRecord =
        LocationRecord.newBuilder()
            .setId(TEST_ID)
            .setDecimalLatitude(30.0)
            .setDecimalLongitude(-20.0)
            .build();

    // When
    ParsedField<Continent> result =
        ContinentParser.parseContinent(extendedRecord, locationRecord, getGeocodeKvStore());

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertNull(result.getResult());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void inTheOceanMismatchTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create().id(TEST_ID).continent("Oceania").build();
    LocationRecord locationRecord =
        LocationRecord.newBuilder()
            .setId(TEST_ID)
            .setDecimalLatitude(30.0)
            .setDecimalLongitude(-20.0)
            .build();

    // When
    ParsedField<Continent> result =
        ContinentParser.parseContinent(extendedRecord, locationRecord, getGeocodeKvStore());

    // Should
    Assert.assertTrue(result.isSuccessful());
    Assert.assertNull(result.getResult());
    Assert.assertTrue(result.getIssues().contains(CONTINENT_COORDINATE_MISMATCH.name()));
    Assert.assertEquals(1, result.getIssues().size());
  }

  @Test(expected = NullPointerException.class)
  public void nullArgsTest() {
    // When
    ContinentParser.parseContinent(null, null, null);
  }
}
