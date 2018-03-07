package org.gbif.pipelines.transform;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * As an example you can convert PCollection<KV<K, V>> to PCollection<V>
 */
public class Kv2Value<K, V> extends PTransform<PCollection<KV<K, V>>, PCollection<V>> {

  public static <K, V> Kv2Value<K, V> create() {
    return new Kv2Value<>();
  }

  private Kv2Value() { }

  @Override
  public PCollection<V> expand(PCollection<KV<K, V>> in) {
    return in.apply("Kv2Value", MapElements.via(new SimpleFunction<KV<K, V>, V>() {
      @Override
      public V apply(KV<K, V> kv) {
        return kv.getValue();
      }
    }));
  }

}
