package org.gbif.pipelines.transforms.core;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.io.avro.LocationRecord;

public class ParentIdTransform {

  /** Maps {@link LocationRecord} to key value, where key is {@link LocationRecord#getParentId} */
  public MapElements<LocationRecord, KV<String, LocationRecord>> toParentKv() {
    return MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
        .via((LocationRecord lr) -> KV.of(lr.getParentId(), lr));
  }
}
