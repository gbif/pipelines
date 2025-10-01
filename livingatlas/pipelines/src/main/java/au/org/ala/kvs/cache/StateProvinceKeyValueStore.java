package au.org.ala.kvs.cache;

import au.org.ala.kvs.GeocodeShpConfig;
import au.org.ala.kvs.client.GeocodeShpIntersectService;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.rest.client.geocode.GeocodeResponse;

public class StateProvinceKeyValueStore implements KeyValueStore<GeocodeRequest, GeocodeResponse> {

  private final GeocodeShpIntersectService service;

  private StateProvinceKeyValueStore(GeocodeShpConfig config) {
    this.service = GeocodeShpIntersectService.getInstance(config);
  }

  public static StateProvinceKeyValueStore create(GeocodeShpConfig config) {
    return new StateProvinceKeyValueStore(config);
  }

  @Override
  public void close() {
    // NOP
  }

  @Override
  public GeocodeResponse get(GeocodeRequest latLng) {
    return new GeocodeResponse(service.lookupStateProvince(latLng.getLat(), latLng.getLng()));
  }
}
