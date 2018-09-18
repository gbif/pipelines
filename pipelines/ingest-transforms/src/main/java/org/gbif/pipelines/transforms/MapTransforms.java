package org.gbif.pipelines.transforms;

import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Set of different map functions */
public class MapTransforms {

  private MapTransforms() {}

  /** Maps {@link ExtendedRecord} to key value, where key is {@link ExtendedRecord#getId} */
  public static MapElements<ExtendedRecord, KV<String, ExtendedRecord>> extendedToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ExtendedRecord>>() {})
        .via((ExtendedRecord ex) -> KV.of(ex.getId(), ex));
  }

  /** Maps {@link BasicRecord} to key value, where key is {@link BasicRecord#getId} */
  public static MapElements<BasicRecord, KV<String, BasicRecord>> basicToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, BasicRecord>>() {})
        .via((BasicRecord br) -> KV.of(br.getId(), br));
  }

  /** Maps {@link TemporalRecord} to key value, where key is {@link TemporalRecord#getId} */
  public static MapElements<TemporalRecord, KV<String, TemporalRecord>> temporalToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, TemporalRecord>>() {})
        .via((TemporalRecord tr) -> KV.of(tr.getId(), tr));
  }

  /** Maps {@link LocationRecord} to key value, where key is {@link LocationRecord#getId} */
  public static MapElements<LocationRecord, KV<String, LocationRecord>> locationToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
        .via((LocationRecord lr) -> KV.of(lr.getId(), lr));
  }

  /** Maps {@link TaxonRecord} to key value, where key is {@link TaxonRecord#getId} */
  public static MapElements<TaxonRecord, KV<String, TaxonRecord>> taxonToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, TaxonRecord>>() {})
        .via((TaxonRecord tr) -> KV.of(tr.getId(), tr));
  }

  /** Maps {@link MultimediaRecord} to key value, where key is {@link MultimediaRecord#getId} */
  public static MapElements<MultimediaRecord, KV<String, MultimediaRecord>> multimediaToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, MultimediaRecord>>() {})
        .via((MultimediaRecord mr) -> KV.of(mr.getId(), mr));
  }
}
