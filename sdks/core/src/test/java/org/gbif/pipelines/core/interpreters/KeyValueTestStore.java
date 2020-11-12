package org.gbif.pipelines.core.interpreters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.rest.client.geocode.GeocodeResponse;

public class KeyValueTestStore implements KeyValueStore<LatLng, GeocodeResponse>, Serializable {

  private final Map<LatLng, GeocodeResponse> map = new HashMap<>();

  @Override
  public GeocodeResponse get(LatLng latLng) {
    return map.get(latLng);
  }

  @Override
  public void close() {}

  public void put(LatLng latLng, GeocodeResponse value) {
    map.put(latLng, value);
  }
}
