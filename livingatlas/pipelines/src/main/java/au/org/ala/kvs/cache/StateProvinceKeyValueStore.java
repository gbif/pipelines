package au.org.ala.kvs.cache;

import au.org.ala.kvs.GeocodeShpConfig;
import au.org.ala.kvs.client.GeocodeShpIntersectService;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.GeocodeService;

import java.io.IOException;

public class StateProvinceKeyValueStore implements KeyValueStore<LatLng, GeocodeResponse> {

  private final GeocodeShpIntersectService service;

  private StateProvinceKeyValueStore(GeocodeShpConfig config) {
    this.service = GeocodeShpIntersectService.getInstance(config);
  }

  public static StateProvinceKeyValueStore create(GeocodeShpConfig config) {
    return new StateProvinceKeyValueStore(config);
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public GeocodeResponse get(LatLng latLng) {
    return new GeocodeResponse(service.lookupStateProvince(latLng.getLatitude(), latLng.getLongitude()));
  }
}
