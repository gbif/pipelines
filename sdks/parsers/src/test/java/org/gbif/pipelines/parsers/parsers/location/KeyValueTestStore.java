package org.gbif.pipelines.parsers.parsers.location;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;

public class KeyValueTestStore implements KeyValueStore<LatLng, String>, Serializable {

  private final Map<LatLng, String> map = new HashMap<>();

  @Override
  public String get(LatLng latLng) {
    return map.get(latLng);
  }

  @Override
  public void close() {
  }

  void put(LatLng latLng, String value) {
    map.put(latLng, value);
  }

}
