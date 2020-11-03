package au.org.ala.kvs;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import au.org.ala.kvs.cache.GeocodeKvStoreFactory;
import au.org.ala.kvs.cache.StateProvinceKeyValueStore;
import au.org.ala.kvs.client.GeocodeShpIntersectService;
import au.org.ala.util.TestUtils;
import java.awt.image.BufferedImage;
import java.util.Collection;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.location.GeocodeKvStore;
import org.gbif.pipelines.factory.BufferedImageFactory;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;
import org.junit.Test;

@Slf4j
public class GeocodeServiceTestIT {

  /**
   * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a
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

  /**
   * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a
   * HashMap.
   */
  @Test
  public void testInsideEEZ() {
    KeyValueStore<LatLng, GeocodeResponse> geoService =
        GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();
    GeocodeResponse resp =
        geoService.get(LatLng.builder().withLongitude(151.329751).withLatitude(-36.407357).build());
    assertFalse(resp.getLocations().isEmpty());
    assertEquals("AU", resp.getLocations().iterator().next().getIsoCountryCode2Digit());
  }

  /**
   * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a
   * HashMap.
   */
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

  /**
   * This test demonstrates how to create a kvstore supporting bitmap cache
   *
   * @throws Exception
   */
  @Test
  public void testBitMap() throws Exception {

    BufferedImage image =
        BufferedImageFactory.loadImageFile("/tmp/pipelines-shp/cw_state_poly.png");
    // Create a KV store (From SHP file or others)
    KeyValueStore<LatLng, GeocodeResponse> stateProvinceStore =
        StateProvinceKeyValueStore.create(TestUtils.getConfig().getGeocodeConfig());
    // Bind bitmap to provide bitmap cache
    KeyValueStore<LatLng, GeocodeResponse> stateProvinceKvStore =
        GeocodeKvStore.create(stateProvinceStore, image);

    GeocodeResponse resp =
        stateProvinceKvStore.get(LatLng.builder().withLongitude(146.2).withLatitude(-27.9).build());
    // Check if search from BitMap
    resp =
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
