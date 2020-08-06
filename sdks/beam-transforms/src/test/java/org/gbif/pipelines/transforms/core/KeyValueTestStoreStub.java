package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.gbif.kvs.KeyValueStore;

public class KeyValueTestStoreStub<K, V> implements KeyValueStore<K, V>, Serializable {

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
