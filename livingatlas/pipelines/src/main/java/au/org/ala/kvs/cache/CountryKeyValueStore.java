package au.org.ala.kvs.cache;

import au.org.ala.kvs.GeocodeShpConfig;
import au.org.ala.kvs.client.GeocodeShpIntersectService;
import java.util.List;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.GeocodeResponse.Location;

public class CountryKeyValueStore implements KeyValueStore<GeocodeRequest, GeocodeResponse> {

  private final GeocodeShpIntersectService service;
  private GeocodeShpConfig config;

  private CountryKeyValueStore(GeocodeShpConfig config) {
    this.service = GeocodeShpIntersectService.getInstance(config);
    this.config = config;
  }

  public static CountryKeyValueStore create(GeocodeShpConfig config) {
    return new CountryKeyValueStore(config);
  }

  @Override
  public void close() {
    // NOP
  }

  @Override
  public GeocodeResponse get(GeocodeRequest latLng) {
    List<Location> locations = service.lookupCountry(latLng.getLat(), latLng.getLng());
    if (locations != null && !locations.isEmpty()) {
      for (Location location : locations) {
        if (location.getName() != null
            && config.getCountry().getIntersectMapping() != null
            && config.getCountry().getIntersectMapping().containsKey(location.getName())) {
          String mappedCode = config.getCountry().getIntersectMapping().get(location.getName());
          // remap
          location.setName(mappedCode);
          location.setId(mappedCode);
          location.setIsoCountryCode2Digit(mappedCode);
        }
      }
    }
    return new GeocodeResponse(locations);
  }
}
