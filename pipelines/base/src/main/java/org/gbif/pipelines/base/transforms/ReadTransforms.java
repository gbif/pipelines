package org.gbif.pipelines.base.transforms;

import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.beam.sdk.io.AvroIO;

/** TODO: DOC */
public class ReadTransforms {

  private ReadTransforms() {}

  /** TODO: DOC */
  public static AvroIO.Read<BasicRecord> basic(String path) {
    return AvroIO.read(BasicRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<TemporalRecord> temporal(String path) {
    return AvroIO.read(TemporalRecord.class).from(path);
  }
  /** TODO: DOC */
  public static AvroIO.Read<TaxonRecord> taxon(String path) {
    return AvroIO.read(TaxonRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<LocationRecord> location(String path) {
    return AvroIO.read(LocationRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<MultimediaRecord> multimedia(String path) {
    return AvroIO.read(MultimediaRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<ExtendedRecord> verbatim(String path) {
    return AvroIO.read(ExtendedRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<MetadataRecord> metadata(String path) {
    return AvroIO.read(MetadataRecord.class).from(path);
  }
}
