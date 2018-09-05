package org.gbif.pipelines.base.transforms;

import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

/** TODO: DOC */
public class MapTransforms {

  private MapTransforms() {}

  /** TODO: DOC */
  public static MapElements<ExtendedRecord, KV<String, ExtendedRecord>> extendedToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ExtendedRecord>>() {})
        .via((ExtendedRecord ex) -> KV.of(ex.getId(), ex));
  }

  /** TODO: DOC */
  public static MapElements<BasicRecord, KV<String, BasicRecord>> basicToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, BasicRecord>>() {})
        .via((BasicRecord br) -> KV.of(br.getId(), br));
  }

  /** TODO: DOC */
  public static MapElements<TemporalRecord, KV<String, TemporalRecord>> temporalToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, TemporalRecord>>() {})
        .via((TemporalRecord tr) -> KV.of(tr.getId(), tr));
  }

  /** TODO: DOC */
  public static MapElements<LocationRecord, KV<String, LocationRecord>> locationToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
        .via((LocationRecord lr) -> KV.of(lr.getId(), lr));
  }

  /** TODO: DOC */
  public static MapElements<TaxonRecord, KV<String, TaxonRecord>> taxonToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, TaxonRecord>>() {})
        .via((TaxonRecord tr) -> KV.of(tr.getId(), tr));
  }

  /** TODO: DOC */
  public static MapElements<MultimediaRecord, KV<String, MultimediaRecord>> multimediaToKv() {
    return MapElements.into(new TypeDescriptor<KV<String, MultimediaRecord>>() {})
        .via((MultimediaRecord mr) -> KV.of(mr.getId(), mr));
  }
}
