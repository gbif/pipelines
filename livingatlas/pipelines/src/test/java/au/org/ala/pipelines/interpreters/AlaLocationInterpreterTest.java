package au.org.ala.pipelines.interpreters;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.LocationInfoConfig;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.rest.client.geocode.Location;

import org.gbif.pipelines.parsers.parsers.location.GeocodeKvStore;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;

import static org.junit.Assert.*;


public class AlaLocationInterpreterTest {

  private static final String ID = "777";
  private ALAPipelinesConfig alaConfig;

  @Before
  public void set(){
    alaConfig = new ALAPipelinesConfig();
    alaConfig.setLocationInfoConfig(new LocationInfoConfig(null,null, null));
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
    //Have to run
    LocationInterpreter.interpretDepth(er, lr);

    LocationInterpreter.interpretContinent(er, lr);
    LocationInterpreter.interpretWaterBody(er, lr);
    LocationInterpreter.interpretMinimumElevationInMeters(er, lr);
    LocationInterpreter.interpretMaximumElevationInMeters(er, lr);
    //Elevation and depth is calculated by min/max depth, elev?
    LocationInterpreter.interpretElevation(er, lr);

    LocationInterpreter.interpretMinimumDistanceAboveSurfaceInMeters(er, lr);
    LocationInterpreter.interpretMaximumDistanceAboveSurfaceInMeters(er, lr);
    LocationInterpreter.interpretCoordinatePrecision(er, lr);
    LocationInterpreter.interpretCoordinateUncertaintyInMeters(er, lr);

    LocationInterpreter.interpretElevation(er, lr);

    //should
    assertEquals(lr.getStateProvince(), "Act");
    assertEquals(lr.getMinimumDepthInMeters(), Double.valueOf(10d));
    assertEquals(lr.getMaximumDepthInMeters(), Double.valueOf(200d));
    //Auto calculated
    assertEquals("Average of Min/Max depth", lr.getDepth(), Double.valueOf(105d));
    assertEquals(lr.getContinent(), "ASIA");
    assertEquals(lr.getWaterBody(), "Murray");
    assertEquals(lr.getMaximumElevationInMeters(), Double.valueOf(2000d));
    assertEquals(lr.getMinimumElevationInMeters(), Double.valueOf(0d));

    assertEquals(lr.getElevation(), Double.valueOf(1000d));
    assertEquals(lr.getMinimumDistanceAboveSurfaceInMeters(), Double.valueOf(14d));
    assertEquals(lr.getMaximumDistanceAboveSurfaceInMeters(), Double.valueOf(200d));
    assertEquals(lr.getCoordinatePrecision(), Double.valueOf(0.5d));
    assertEquals(lr.getCoordinateUncertaintyInMeters(), Double.valueOf(1d));

    ALALocationInterpreter.interpretGeoreferencedDate(er, lr);
    ALALocationInterpreter.interpretGeoreferenceTerms(er, lr);
    assertEquals("1979-01-01T00:00", lr.getGeoreferencedDate());
    assertEquals(lr.getIssues().getIssueList().size(), 4);
  }

  @Test
  public void assertionElevationTest() {
    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    coreMap.put(DwcTerm.coordinatePrecision.qualifiedName(), "100");
    LocationInterpreter.interpretCoordinatePrecision(er, lr);
    assertEquals(lr.getIssues().getIssueList().get(0), "COORDINATE_PRECISION_INVALID");

    coreMap.put(DwcTerm.minimumElevationInMeters.qualifiedName(), " we 0 test 1000 inch");
    LocationInterpreter.interpretMinimumElevationInMeters(er, lr);

    coreMap.put(DwcTerm.maximumElevationInMeters.qualifiedName(), " we 1 test 3 meter");
    LocationInterpreter.interpretMaximumElevationInMeters(er, lr);
    LocationInterpreter.interpretElevation(er, lr);

    assertArrayEquals(lr.getIssues().getIssueList().toArray(),
        new String[]{
                "COORDINATE_PRECISION_INVALID",
                OccurrenceIssue.ELEVATION_MIN_MAX_SWAPPED.name(),
                "ELEVATION_NOT_METRIC",
                "ELEVATION_NON_NUMERIC"}
        );
  }

  @Test
  public void assertionMaximumDistanceAboveSurfaceInMetersTest() {
    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    coreMap.put(DwcTerm.maximumDistanceAboveSurfaceInMeters.qualifiedName(), "2err0 inch");
    LocationInterpreter.interpretMaximumDistanceAboveSurfaceInMeters(er, lr);

    assertEquals(lr.getMaximumDistanceAboveSurfaceInMeters(), Double.valueOf(0.51d));
  }

  @Test
  public void assertionMissingGeodeticTest() {
    Location state = new Location();
    state.setName("New South Wales");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(new LatLng(-31.25d, 146.921099d),
        new GeocodeResponse(Collections.singletonList(state)));

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    coreMap.put(DwcTerm.verbatimLatitude.qualifiedName(), "-31.25d");
    coreMap.put(DwcTerm.verbatimLongitude.qualifiedName(), "146.921099d");

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);
    ALALocationInterpreter.verifyLocationInfo(alaConfig).accept(er,lr);

    assertEquals(lr.getStateProvince(), "New South Wales");

    assertArrayEquals(lr.getIssues().getIssueList().toArray(),
        new String[]{
                OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84.name(),
                ALAOccurrenceIssue.MISSING_GEODETICDATUM.name(),
                ALAOccurrenceIssue.COORDINATES_CENTRE_OF_STATEPROVINCE.name()
        });
  }

  @Test
  public void coordinatesCentreOfStateProvinceTest() {
    Location state = new Location();
    state.setName("New South Wales");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(new LatLng(-31.25d, 146.921099d),
        new GeocodeResponse(Collections.singletonList(state)));

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    coreMap.put(DwcTerm.verbatimLatitude.qualifiedName(), "-31.25d");
    coreMap.put(DwcTerm.verbatimLongitude.qualifiedName(), "146.921099d");
    coreMap.put(DwcTerm.geodeticDatum.qualifiedName(), "WGS84");

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);
    ALALocationInterpreter.verifyLocationInfo(alaConfig).accept(er,lr);

    assertEquals("New South Wales", lr.getStateProvince());

    assertArrayEquals(lr.getIssues().getIssueList().toArray(),
        new String[]{ALAOccurrenceIssue.COORDINATES_CENTRE_OF_STATEPROVINCE.name()});
  }

  @Test
  public void assertionInvalidGeodeticTest() {
    Location state = new Location();
    state.setName("New South Wales");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(new LatLng(-31.25d, 146.921099d),
        new GeocodeResponse(Collections.singletonList(state)));

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    coreMap.put(DwcTerm.verbatimLatitude.qualifiedName(), "-31.25d");
    coreMap.put(DwcTerm.verbatimLongitude.qualifiedName(), "146.921099d");
    coreMap.put(DwcTerm.geodeticDatum.qualifiedName(), "TEST");

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);
    ALALocationInterpreter.verifyLocationInfo(alaConfig).accept(er,lr);

    assertEquals(lr.getStateProvince(), "New South Wales");

    assertArrayEquals(lr.getIssues().getIssueList().toArray(),
        new String[]{
                OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84.name(),
                OccurrenceIssue.GEODETIC_DATUM_INVALID.name(),
                ALAOccurrenceIssue.COORDINATES_CENTRE_OF_STATEPROVINCE.name(),
        });
  }

  @Test
  public void assertionStateProvinceInvalidAssertionTest() {
    Location state = new Location();
    state.setName("Victoria");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(
            new LatLng(-37.47d, 144.785153d),
            new GeocodeResponse(Collections.singletonList(state))
    );

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    //swapped lat/lng
    //note: GBIF will truncate coordinates to 6 decimal places...
    coreMap.put(DwcTerm.verbatimLatitude.qualifiedName(), "144.7851531");
    coreMap.put(DwcTerm.verbatimLongitude.qualifiedName(), "-37.47");

    coreMap.put(DwcTerm.geodeticDatum.qualifiedName(), "WGS84");
    coreMap.put(DwcTerm.stateProvince.qualifiedName(), "New South Wales");

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);
    assertEquals("Victoria", lr.getStateProvince());

    assertArrayEquals(lr.getIssues().getIssueList().toArray(),
        new String[]{
                OccurrenceIssue.COORDINATE_ROUNDED.name(),
                OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE.name(),
                ALAOccurrenceIssue.STATE_COORDINATE_MISMATCH.name()
    });
  }

  @Test
  public void assertionMissingLocationTest() {
    Location state = new Location();
    state.setName("New South Wales");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(new LatLng(-31.2532183d, 146.921099d),
        new GeocodeResponse(Collections.singletonList(state)));

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);

    assertArrayEquals(lr.getIssues().getIssueList().toArray(),
        new String[]{ALAOccurrenceIssue.LOCATION_NOT_SUPPLIED.name()});
  }

  @Test
  public void assertionZeroCoordinateTest() {
    Location state = new Location();
    state.setName("New South Wales");
    state.setType("State");

    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(new LatLng(-31.2532183d, 146.921099d),
        new GeocodeResponse(Collections.singletonList(state)));


    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    Map<String, String> coreMap = new HashMap<>();

    coreMap.put(DwcTerm.decimalLatitude.qualifiedName(), "0");
    coreMap.put(DwcTerm.decimalLongitude.qualifiedName(), "0");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    ALALocationInterpreter.interpretStateProvince(kvStore).accept(er, lr);

    assertNull(lr.getStateProvince());

    assertArrayEquals(lr.getIssues().getIssueList().toArray(),
        new String[]{OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84.name(), ALAOccurrenceIssue.MISSING_GEODETICDATUM.name() ,OccurrenceIssue.ZERO_COORDINATE.name()});
  }

  @Test
  public void assertCountryCoordinateTest() {

    KeyValueTestStoreStub store = new KeyValueTestStoreStub();
    store.put(new LatLng(15.958333d, -85.908333d), createCountryResponse(Country.HONDURAS));
    store.put(new LatLng(-2.752778d, -58.653057d), createCountryResponse(Country.BRAZIL));
//    store.put(new LatLng(-2.752778d, -58.653057d),
//            createStateAndCountryResponse("San Luise", "Brazil", "BR"));

    MetadataRecord mdr = MetadataRecord.newBuilder().setId(ID).build();

    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.verbatimLatitude.qualifiedName(), "-2.752778d");
    coreMap.put(DwcTerm.verbatimLongitude.qualifiedName(), "-58.653057d");
    coreMap.put(DwcTerm.geodeticDatum.qualifiedName(), "EPSG:4326");

    ExtendedRecord source = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    Optional<LocationRecord> lrResult = Interpretation.from(source)
        .to(er -> LocationRecord.newBuilder().setId(er.getId()).build())
        .via(LocationInterpreter.interpretCountryAndCoordinates(store, mdr))
        .get();

    //country matches
    LocationRecord lr = lrResult.get();
    assertEquals(lr.getIssues().getIssueList().size(), 1); //country derived from coordinates
    assertEquals(Country.BRAZIL.getIso2LetterCode(), lr.getCountryCode());
    assertEquals(Country.BRAZIL.getTitle(), lr.getCountry());
  }

  /**
   * Only works for country
   */
  private static GeocodeResponse createCountryResponse(Country country) {
    Location location = new Location();
    location.setIsoCountryCode2Digit(country.getIso2LetterCode());
    location.setType("Political");
    return new GeocodeResponse(Collections.singletonList(location));
  }


  private class KeyValueTestStoreStub<K, V> implements KeyValueStore<K, V>, Serializable {

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
