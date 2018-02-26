package org.gbif.pipelines.core.utils;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

public class Mapper {

  private Mapper() {
    // Can't have an instance
  }

  /**
   *
   */
  public static <T, R> MapElements<T, R> via(SerializableFunction<T, R> function) {
    return MapElements.into(new TypeDescriptor<R>() {}).via(function);
  }

  /**
   * Map a key value collection to a value collection, PCollection<KV<String, Object>> to PCollection<Object>
   */
  public static <T> MapElements<KV<String, T>, T> toValueCollection() {
    return via(KV::getValue);
  }

}
