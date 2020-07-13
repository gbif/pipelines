package au.org.ala.kvs;

import au.org.ala.kvs.cache.GeocodeKvStoreFactory;
import au.org.ala.kvs.client.GeocodeShpIntersectService;
import au.org.ala.util.TestUtils;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;
import org.junit.Test;

import java.util.Collection;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class GeocodeServiceTest {

    /**
     * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a HashMap.
     */
    @Test
    public void testInsideCountry() throws Exception {
        KeyValueStore<LatLng,GeocodeResponse> geoService = GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();
        GeocodeResponse resp = geoService.get(LatLng.builder().withLongitude(146.2).withLatitude(-27.9).build());
        assertFalse(resp.getLocations().isEmpty());
        Collection<Location> locations = resp.getLocations();
        assertFalse(locations.isEmpty());

        Optional<Location> country = locations.stream().filter(l -> l.getType().equals(GeocodeShpIntersectService.POLITICAL_LOCATION_TYPE)).findFirst();
        assertTrue(country.isPresent());
        assertEquals(country.get().getCountryName(), "AU");
    }

    /**
     * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a HashMap.
     */
    @Test
    public void testInsideEEZ() throws Exception {
        KeyValueStore<LatLng,GeocodeResponse> geoService = GeocodeKvStoreFactory.createCountrySupplier(TestUtils.getConfig()).get();
        GeocodeResponse resp = geoService.get(LatLng.builder().withLongitude(151.329751).withLatitude(-36.407357).build());
        assertFalse(resp.getLocations().isEmpty());
        assertEquals(resp.getLocations().iterator().next().getCountryName(), "AU");
    }

    /**
     * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a HashMap.
     */
    @Test
    public void testInsideStateProvince() throws Exception {
        KeyValueStore<LatLng,GeocodeResponse> geoService = GeocodeKvStoreFactory.createStateProvinceSupplier(TestUtils.getConfig()).get();
        GeocodeResponse resp = geoService.get(LatLng.builder().withLongitude(146.2).withLatitude(-27.9).build());
        assertFalse(resp.getLocations().isEmpty());
        Collection<Location> locations = resp.getLocations();
        assertFalse(locations.isEmpty());

        Optional<Location> stateProvince = locations.stream().filter(l -> l.getType().equals(GeocodeShpIntersectService.STATE_PROVINCE_LOCATION_TYPE)).findFirst();
        assertTrue(stateProvince.isPresent());
        assertEquals("Queensland", stateProvince.get().getName());
    }
}
