package org.gbif.pipelines.core.parsers.location;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.rest.client.geocode.GeocodeResponse;

public class KeyValueTestStore
    implements KeyValueStore<GeocodeRequest, GeocodeResponse>, Serializable {

  private final Map<GeocodeRequest, GeocodeResponse> map = new HashMap<>();

  @Override
  public GeocodeResponse get(GeocodeRequest latLng) {
    return map.get(latLng);
  }

  @Override
  public void close() {}

  void put(GeocodeRequest latLng, GeocodeResponse value) {
    map.put(latLng, value);
  }
}
