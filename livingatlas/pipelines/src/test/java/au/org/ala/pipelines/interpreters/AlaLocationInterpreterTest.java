package au.org.ala.pipelines.interpreters;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.LocationInfoConfig;
import au.org.ala.pipelines.vocabulary.*;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class AlaLocationInterpreterTest {

  private static final String ID = "777";

  private CentrePoints countryCentrePoints;
  private CentrePoints stateProvinceCentrePoints;
  private StateProvinceParser stateProvinceParser;

  @Before
  public void set() {
    ALAPipelinesConfig alaConfig = new ALAPipelinesConfig();
    alaConfig.setLocationInfoConfig(new LocationInfoConfig(null, null, null));
    try {
      countryCentrePoints = CountryCentrePoints.getInstance(alaConfig.getLocationInfoConfig());
      stateProvinceCentrePoints =
          StateProvinceCentrePoints.getInstance(alaConfig.getLocationInfoConfig());
      stateProvinceParser =
          StateProvinceParser.getInstance(
              alaConfig.getLocationInfoConfig().getStateProvinceNamesFile());
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new PipelinesException(e.getMessage());
    }
  }

  @Test
  public void biomeTest() {
    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    Location terrestrial = new Location();
    terrestrial.setName("Terrestrial");
    kvStore.put(
        new LatLng(-31.25d, 146.921099d),
        new GeocodeResponse(Collections.singletonList(terrestrial)));

    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.decimalLatitude.qualifiedName(), "-31.25");
    coreMap.put(DwcTerm.decimalLongitude.qualifiedName(), "146.921099");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    ALALocationInterpreter.interpretBiome(kvStore).accept(er, lr);

    assertEquals("Terrestrial", lr.getBiome());
  }

  @Test
  public void gbifAlaTest() {

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();

    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.stateProvince.qualifiedName(), "ACT");
    coreMap.put(DwcTerm.minimumDepthInMeters.qualifiedName(), "10");
    coreMap.put(DwcTerm.maximumDepthInMeters.qualifiedName(), "200");
    coreMap.put(DwcTerm.continent.qualifiedName(), "Asia");
    coreMap.put(DwcTerm.waterBody.qualifiedName(), "Murray");
    coreMap.put(DwcTerm.minimumElevationInMeters.qualifiedName(), "0");
    coreMap.put(DwcTerm.maximumElevationInMeters.qualifiedName(), "2000");
    coreMap.put(DwcTerm.minimumDistanceAboveSurfaceInMeters.qualifiedName(), "1 test 4 meter");
    coreMap.put(DwcTerm.maximumDistanceAboveSurfaceInMeters.qualifiedName(), "200");
    coreMap.put(DwcTerm.coordinatePrecision.qualifiedName(), "0.5");
    coreMap.put(DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), "1");
    coreMap.put(DwcTerm.geodeticDatum.qualifiedName(), "EPSG:4326");
    coreMap.put(DwcTerm.georeferencedDate.qualifiedName(), "1979-1-1");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    LocationInterpreter.interpretStateProvince(er, lr);
    LocationInterpreter.interpretMinimumDepthInMeters(er, lr);
    LocationInterpreter.interpretMaximumDepthInMeters(er, lr);
    // Have to run
    LocationInterpreter.interpretDepth(er, lr);

    LocationInterpreter.interpretContinent(new KeyValueTestStoreStub<>()).accept(er, lr);
    LocationInterpreter.interpretWaterBody(er, lr);
    LocationInterpreter.interpretMinimumElevationInMeters(er, lr);
    LocationInterpreter.interpretMaximumElevationInMeters(er, lr);
    // Elevation and depth is calculated by min/max depth, elev?
    LocationInterpreter.interpretElevation(er, lr);

    LocationInterpreter.interpretMinimumDistanceAboveSurfaceInMeters(er, lr);
    LocationInterpreter.interpretMaximumDistanceAboveSurfaceInMeters(er, lr);
    LocationInterpreter.interpretCoordinatePrecision(er, lr);
    LocationInterpreter.interpretCoordinateUncertaintyInMeters(er, lr);

    ALALocationInterpreter.builder().create().interpretGeoreferencedDate(er, lr);
    ALALocationInterpreter.interpretGeoreferenceTerms(er, lr);
    assertEquals("1979-01-01T00:00", lr.getGeoreferencedDate());
    assertEquals(4, lr.getIssues().getIssueList().size(), 1);
  }

  @Test
  public void assertionElevationPrecisionTest() {
    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    coreMap.put(DwcTerm.coordinatePrecision.qualifiedName(), "100");
    LocationInterpreter.interpretCoordinatePrecision(er, lr);

    assertEquals("COORDINATE_PRECISION_INVALID", lr.getIssues().getIssueList().get(0));

    coreMap.put(DwcTerm.minimumElevationInMeters.qualifiedName(), " we 0 test 1000 inch");
    LocationInterpreter.interpretMinimumElevationInMeters(er, lr);

    coreMap.put(DwcTerm.maximumElevationInMeters.qualifiedName(), " we 1 test 3 meter");
    LocationInterpreter.interpretMaximumElevationInMeters(er, lr);
    LocationInterpreter.interpretElevation(er, lr);
    ALALocationInterpreter.interpretCoordinateUncertaintyInMeters(er, lr);

    assertArrayEquals(
        new String[] {
          "COORDINATE_PRECISION_INVALID",
          OccurrenceIssue.ELEVATION_MIN_MAX_SWAPPED.name(),
          "ELEVATION_NOT_METRIC",
          "ELEVATION_NON_NUMERIC",
          OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID.name(),
          ALAOccurrenceIssue.UNCERTAINTY_IN_PRECISION.name()
        },
        lr.getIssues().getIssueList().toArray());
  }

  @Test
  public void assertionMaximumDistanceAboveSurfaceInMetersTest() {
    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    coreMap.put(DwcTerm.maximumDistanceAboveSurfaceInMeters.qualifiedName(), "2err0 inch");
    LocationInterpreter.interpretMaximumDistanceAboveSurfaceInMeters(er, lr);

    assertEquals(Double.valueOf(0.51d), lr.getMaximumDistanceAboveSurfaceInMeters());
  }

  /** Tests on: Missing geodetic datum precision mismatch Centre of state */
  @Test
  public void assertionMissingGeodeticTest() {
    Location state = new Location();
    state.setName("New South Wales");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(
        new LatLng(-31.25d, 146.921099d), new GeocodeResponse(Collections.singletonList(state)));

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    coreMap.put(DwcTerm.verbatimLatitude.qualifiedName(), "-31.25d");
    coreMap.put(DwcTerm.verbatimLongitude.qualifiedName(), "146.921099d");

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);
    ALALocationInterpreter.verifyLocationInfo(
            countryCentrePoints, stateProvinceCentrePoints, stateProvinceParser)
        .accept(er, lr);

    assertEquals("New South Wales", lr.getStateProvince());

    assertArrayEquals(
        new String[] {
          OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84.name(),
          ALAOccurrenceIssue.MISSING_GEODETICDATUM.name(),
          ALAOccurrenceIssue.COORDINATES_CENTRE_OF_STATEPROVINCE.name()
        },
        lr.getIssues().getIssueList().toArray());
  }

  @Test
  public void coordinatesCentreOfStateProvinceTest() {
    Location state = new Location();
    state.setName("New South Wales");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(
        new LatLng(-31.25d, 146.921099d), new GeocodeResponse(Collections.singletonList(state)));

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    coreMap.put(DwcTerm.verbatimLatitude.qualifiedName(), "-31.25d");
    coreMap.put(DwcTerm.verbatimLongitude.qualifiedName(), "146.921099d");
    coreMap.put(DwcTerm.geodeticDatum.qualifiedName(), "WGS84");

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);
    ALALocationInterpreter.verifyLocationInfo(
            countryCentrePoints, stateProvinceCentrePoints, stateProvinceParser)
        .accept(er, lr);

    assertEquals("New South Wales", lr.getStateProvince());

    assertArrayEquals(
        new String[] {ALAOccurrenceIssue.COORDINATES_CENTRE_OF_STATEPROVINCE.name()},
        lr.getIssues().getIssueList().toArray());
  }

  @Test
  public void assertionInvalidGeodeticTest() {
    Location state = new Location();
    state.setName("New South Wales");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(
        new LatLng(-31.25d, 146.921099d), new GeocodeResponse(Collections.singletonList(state)));

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    coreMap.put(DwcTerm.verbatimLatitude.qualifiedName(), "-31.25d");
    coreMap.put(DwcTerm.verbatimLongitude.qualifiedName(), "146.921099d");
    coreMap.put(DwcTerm.geodeticDatum.qualifiedName(), "TEST");

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);
    ALALocationInterpreter.verifyLocationInfo(
            countryCentrePoints, stateProvinceCentrePoints, stateProvinceParser)
        .accept(er, lr);

    assertEquals("New South Wales", lr.getStateProvince());

    assertArrayEquals(
        new String[] {
          OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84.name(),
          OccurrenceIssue.GEODETIC_DATUM_INVALID.name(),
          ALAOccurrenceIssue.COORDINATES_CENTRE_OF_STATEPROVINCE.name(),
        },
        lr.getIssues().getIssueList().toArray());
  }

  /** Check on state variants and related assertions */
  @Test
  public void assertionStateProvinceInvalidAssertionTest() {
    Location state = new Location();
    state.setName("vic");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(
        new LatLng(-37.47d, 144.785153d), new GeocodeResponse(Collections.singletonList(state)));

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    // swapped lat/lng
    // note: GBIF will truncate coordinates to 6 decimal places...
    coreMap.put(DwcTerm.verbatimLatitude.qualifiedName(), "144.7851531");
    coreMap.put(DwcTerm.verbatimLongitude.qualifiedName(), "-37.47");

    coreMap.put(DwcTerm.geodeticDatum.qualifiedName(), "WGS84");
    coreMap.put(DwcTerm.stateProvince.qualifiedName(), "New South Wales");

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);
    ALALocationInterpreter.verifyLocationInfo(
            countryCentrePoints, stateProvinceCentrePoints, stateProvinceParser)
        .accept(er, lr);
    assertEquals("Victoria", lr.getStateProvince());

    assertArrayEquals(
        new String[] {
          OccurrenceIssue.COORDINATE_ROUNDED.name(),
          OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE.name(),
          ALAOccurrenceIssue.STATE_COORDINATE_MISMATCH.name(),
          ALAOccurrenceIssue.COORDINATES_CENTRE_OF_STATEPROVINCE.name()
        },
        lr.getIssues().getIssueList().toArray());
  }

  @Test
  public void assertionMissingLocationTest() {
    Location state = new Location();
    state.setName("New South Wales");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(
        new LatLng(-31.2532183d, 146.921099d),
        new GeocodeResponse(Collections.singletonList(state)));

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);

    assertArrayEquals(
        new String[] {ALAOccurrenceIssue.LOCATION_NOT_SUPPLIED.name()},
        lr.getIssues().getIssueList().toArray());
  }

  @Test
  public void assertionZeroCoordinateTest() {
    Location state = new Location();
    state.setName("New South Wales");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(
        new LatLng(-31.2532183d, 146.921099d),
        new GeocodeResponse(Collections.singletonList(state)));

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    coreMap.put(DwcTerm.decimalLatitude.qualifiedName(), "0");
    coreMap.put(DwcTerm.decimalLongitude.qualifiedName(), "0");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);

    assertNull(lr.getStateProvince());

    assertArrayEquals(
        new String[] {
          OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84.name(),
          ALAOccurrenceIssue.MISSING_GEODETICDATUM.name(),
          OccurrenceIssue.ZERO_COORDINATE.name()
        },
        lr.getIssues().getIssueList().toArray());
  }

  @Test
  public void assertCountryCentre() {
    KeyValueTestStoreStub<LatLng, GeocodeResponse> store = new KeyValueTestStoreStub<>();
    store.put(new LatLng(-29.532804, 145.491477), createCountryResponse(Country.AUSTRALIA));

    MetadataRecord mdr = MetadataRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.verbatimLatitude.qualifiedName(), "-29.532804d");
    coreMap.put(DwcTerm.verbatimLongitude.qualifiedName(), "145.491477d");
    coreMap.put(DwcTerm.geodeticDatum.qualifiedName(), "EPSG:4326");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();

    LocationInterpreter.interpretCountryAndCoordinates(store, mdr).accept(er, lr);
    ALALocationInterpreter.verifyLocationInfo(
            countryCentrePoints, stateProvinceCentrePoints, stateProvinceParser)
        .accept(er, lr);

    assertArrayEquals(
        new String[] {
          OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES.name(),
          ALAOccurrenceIssue.COORDINATES_CENTRE_OF_COUNTRY.name()
        },
        lr.getIssues().getIssueList().toArray());
    assertEquals(Country.AUSTRALIA.getTitle(), lr.getCountry());
  }

  /** Only works for country */
  private static GeocodeResponse createCountryResponse(Country country) {
    Location location = new Location();
    location.setIsoCountryCode2Digit(country.getIso2LetterCode());
    location.setType("Political");
    return new GeocodeResponse(Collections.singletonList(location));
  }

  private static class KeyValueTestStoreStub<K, V> implements KeyValueStore<K, V>, Serializable {

    private final Map<K, V> map = new HashMap<>();

    @Override
    public V get(K key) {
      return map.get(key);
    }

    @Override
    public void close() {
      // NOP
    }

    void put(K key, V value) {
      map.put(key, value);
    }
  }
}
