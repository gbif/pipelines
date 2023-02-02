package au.org.ala.kvs;

import static org.junit.Assert.*;

import au.org.ala.kvs.cache.GeocodeKvStoreFactory;
import au.org.ala.kvs.cache.StateProvinceKeyValueStore;
import au.org.ala.kvs.client.GeocodeShpIntersectService;
import au.org.ala.util.TestUtils;
import java.awt.image.BufferedImage;
import java.util.Collection;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.location.GeocodeKvStore;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.factory.BufferedImageFactory;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;
import org.junit.Test;

@Slf4j
public class GeocodeServiceTestIT {

  /**
   * Tests the Get operation on {@link KeyValueStore} that wraps a simple KV store backed by a
   * HashMap.
   */
  @Test
  public void testCountryCoastalPoint() {

    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();

    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(119.7).withLatitude(-20.0).build());
    assertFalse(resp.getLocations().isEmpty());
    assertEquals("AU", resp.getLocations().get(0).getName());

    GeocodeResponse resp2 =
        geoService.get(
            LatLng.builder().withLongitude(124.9500000000).withLatitude(-15.0667000000).build());
    assertFalse(resp2.getLocations().isEmpty());
    assertEquals("AU", resp2.getLocations().get(0).getName());

    GeocodeResponse resp3 =
        geoService.get(LatLng.builder().withLongitude(115.445278).withLatitude(-20.827222).build());
    assertFalse(resp3.getLocations().isEmpty());
    assertEquals("AU", resp3.getLocations().get(0).getName());
  }

  @Test
  public void testExternalTerritories() {

    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();

    // Christmas Island
    GeocodeResponse resp =
        geoService.get(
            LatLng.builder()
                .withLongitude(105.65916507921472)
                .withLatitude(-10.47444666578651)
                .build());
    assertFalse(resp.getLocations().isEmpty());
    assertEquals("AU", resp.getLocations().get(0).getName());

    // Cocos Island
    GeocodeResponse resp2 =
        geoService.get(
            LatLng.builder()
                .withLongitude(96.82189811041627)
                .withLatitude(-12.149492596153177)
                .build());
    assertFalse(resp2.getLocations().isEmpty());
    assertEquals("AU", resp2.getLocations().get(0).getName());

    // HM Heard Island and McDonald Islands
    GeocodeResponse resp3 =
        geoService.get(
            LatLng.builder()
                .withLongitude(73.50048378875914)
                .withLatitude(-53.05122953207953)
                .build());
    assertFalse(resp3.getLocations().isEmpty());
    assertEquals("AU", resp2.getLocations().get(0).getName());

    // Norfolk Island
    GeocodeResponse resp4 =
        geoService.get(
            LatLng.builder()
                .withLongitude(167.96164271317312)
                .withLatitude(-29.030669709715056)
                .build());
    assertFalse(resp4.getLocations().isEmpty());
    assertEquals("AU", resp4.getLocations().get(0).getName());

    // Lord Howe
    GeocodeResponse resp5 =
        geoService.get(
            LatLng.builder()
                .withLongitude(159.0851385821144)
                .withLatitude(-31.560315624981254)
                .build());
    assertFalse(resp5.getLocations().isEmpty());
    assertEquals("AU", resp5.getLocations().get(0).getName());
  }

  /**
   * Tests the Get operation on {@link KeyValueStore} that wraps a simple KV store backed by a
   * HashMap.
   */
  @Test
  public void testCountryDatelineTests() {

    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();

    // FJ
    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(179.9).withLatitude(-17.99).build());
    assertFalse(resp.getLocations().isEmpty());
    assertEquals("FJ", resp.getLocations().get(0).getName());

    // NZ
    GeocodeResponse resp2 =
        geoService.get(LatLng.builder().withLongitude(179.9).withLatitude(-40.0).build());
    assertFalse(resp2.getLocations().isEmpty());
    assertEquals("NZ", resp2.getLocations().get(0).getName());

    // NZ - other side of dateline
    GeocodeResponse resp3 =
        geoService.get(LatLng.builder().withLongitude(-179.9).withLatitude(-40.0).build());
    assertFalse(resp3.getLocations().isEmpty());
    assertEquals("NZ", resp3.getLocations().get(0).getName());

    // south pole
    GeocodeResponse resp4 =
        geoService.get(LatLng.builder().withLongitude(179.9).withLatitude(-90.0).build());
    assertFalse(resp4.getLocations().isEmpty());
    assertEquals("AQ", resp4.getLocations().get(0).getName());

    // north pole
    GeocodeResponse resp5 =
        geoService.get(LatLng.builder().withLongitude(179.9).withLatitude(90.0).build());
    assertTrue(resp5.getLocations().isEmpty());
  }

  /**
   * Tests the Get operation on {@link KeyValueStore} that wraps a simple KV store backed by a
   * HashMap.
   */
  @Test
  public void testBiomeTerrestrial() {

    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createBiomeSupplier(TestUtils.getConfig()).get();

    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(146.2).withLatitude(-27.9).build());

    assertFalse(resp.getLocations().isEmpty());
    Collection<Location> locations = resp.getLocations();
    assertFalse(locations.isEmpty());

    Optional<Location> biome = locations.stream().findFirst();
    assertTrue(biome.isPresent());
    assertEquals("TERRESTRIAL", biome.get().getName());
  }

  @Test
  public void testBiomeMarine() {

    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createBiomeSupplier(TestUtils.getConfig()).get();

    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(106.6).withLatitude(-31.2).build());

    assertFalse(resp.getLocations().isEmpty());
    Collection<Location> locations = resp.getLocations();
    assertFalse(locations.isEmpty());

    Optional<Location> biome = locations.stream().findFirst();
    assertTrue(biome.isPresent());
    assertEquals("MARINE", biome.get().getName());
  }

  /**
   * Tests the Get operation on {@link KeyValueStore} that wraps a simple KV store backed by a
   * HashMap.
   */
  @Test
  public void testInsideCountry() {
    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();
    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(146.2).withLatitude(-27.9).build());
    assertFalse(resp.getLocations().isEmpty());
    Collection<Location> locations = resp.getLocations();
    assertFalse(locations.isEmpty());

    Optional<Location> country =
        locations.stream()
            .filter(l -> l.getType().equals(GeocodeShpIntersectService.POLITICAL_LOCATION_TYPE))
            .findFirst();
    assertTrue(country.isPresent());
    assertEquals("AU", country.get().getIsoCountryCode2Digit());
  }

  @Test
  public void testInsideEEZ() {
    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();
    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(151.329751).withLatitude(-36.407357).build());
    assertFalse(resp.getLocations().isEmpty());
    assertEquals("AU", resp.getLocations().iterator().next().getIsoCountryCode2Digit());
  }

  @Test
  public void testInsideStateProvince() {
    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createStateProvinceSupplier(TestUtils.getConfig()).get();
    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(146.2).withLatitude(-27.9).build());
    assertFalse(resp.getLocations().isEmpty());
    Collection<Location> locations = resp.getLocations();
    assertFalse(locations.isEmpty());

    Optional<Location> stateProvince =
        locations.stream()
            .filter(
                l -> l.getType().equals(GeocodeShpIntersectService.STATE_PROVINCE_LOCATION_TYPE))
            .findFirst();
    assertTrue(stateProvince.isPresent());
    assertEquals("Queensland", stateProvince.get().getName());

    resp = geoService.get(LatLng.builder().withLongitude(146.923).withLatitude(-31.2).build());
    assertEquals(1, resp.getLocations().size());
    assertEquals("New South Wales", resp.getLocations().iterator().next().getName());
    // Manually Check if the result is from bitmap
    resp = geoService.get(LatLng.builder().withLongitude(146.3).withLatitude(-27.8).build());
    assertEquals("Queensland", stateProvince.get().getName());
  }

  @Test
  public void testOutsideStateProvince() {
    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createStateProvinceSupplier(TestUtils.getConfig()).get();

    GeocodeResponse resp =
        geoService.get(
            LatLng.builder().withLongitude(-145.077283).withLatitude(-38.188337).build());
    assertTrue(resp.getLocations().isEmpty());
  }

  //   -30.060141,151.905301;
  @Test
  public void testCountryEEZ1() {
    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();

    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(146.804061).withLatitude(-35.482884).build());

    assertFalse(resp.getLocations().isEmpty());
  }
  //    -35.482884,146.804061
  @Test
  public void testCountryEEZ2() {
    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();

    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(151.905301).withLatitude(-30.060141).build());

    assertFalse(resp.getLocations().isEmpty());
  }

  // -36.792365,146.074153
  @Test
  public void testCountryEEZ3() {
    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();

    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(146.074153).withLatitude(-36.792365).build());
    assertFalse(resp.getLocations().isEmpty());
  }

  @Test
  public void testOutsideCountryEEZ() {
    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();

    GeocodeResponse resp =
        geoService.get(
            LatLng.builder().withLongitude(-145.077283).withLatitude(-38.188337).build());
    assertTrue(resp.getLocations().isEmpty());
  }

  /** This test demonstrates how to create a kvstore supporting bitmap cache */
  @Test
  public void testBitMap() {

    BufferedImage image =
        BufferedImageFactory.loadImageFile(
            HdfsConfigs.nullConfig(), "/tmp/pipelines-shp/cw_state_poly.png");
    // Create a KV store (From SHP file or others)
    KeyValueStore<LatLng, GeocodeResponse> stateProvinceStore =
        StateProvinceKeyValueStore.create(TestUtils.getConfig().getGeocodeConfig());
    // Bind bitmap to provide bitmap cache
    KeyValueStore<LatLng, GeocodeResponse> stateProvinceKvStore =
        GeocodeKvStore.create(stateProvinceStore, image);

    // Check if search from BitMap
    GeocodeResponse resp =
        stateProvinceKvStore.get(LatLng.builder().withLongitude(146.3).withLatitude(-27.9).build());
    assertEquals(1, resp.getLocations().size());
    assertEquals("Queensland", resp.getLocations().iterator().next().getName());

    resp =
        stateProvinceKvStore.get(
            LatLng.builder().withLongitude(146.921).withLatitude(-31.25).build());
    assertEquals(1, resp.getLocations().size());

    resp =
        stateProvinceKvStore.get(
            LatLng.builder().withLongitude(146.923).withLatitude(-31.2).build());
    assertEquals(1, resp.getLocations().size());
    assertEquals("New South Wales", resp.getLocations().iterator().next().getName());
  }
}
