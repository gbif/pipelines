package org.gbif.pipelines.base.transforms;

import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;

public class WriteTransforms {

  private WriteTransforms() {}

  public static AvroIO.Write<MetadataRecord> metadata(String toPath) {
    return create(MetadataRecord.class, toPath).withoutSharding();
  }

  public static AvroIO.Write<BasicRecord> basic(String toPath) {
    return create(BasicRecord.class, toPath);
  }

  public static AvroIO.Write<TemporalRecord> temporal(String toPath) {
    return create(TemporalRecord.class, toPath);
  }

  public static AvroIO.Write<TaxonRecord> taxon(String toPath) {
    return create(TaxonRecord.class, toPath);
  }

  public static AvroIO.Write<MultimediaRecord> multimedia(String toPath) {
    return create(MultimediaRecord.class, toPath);
  }

  public static AvroIO.Write<LocationRecord> location(String toPath) {
    return create(LocationRecord.class, toPath);
  }

  public static <T> AvroIO.Write<T> create(Class<T> clazz, String toPath) {
    return create(clazz, toPath, CodecFactory.snappyCodec());
  }

  public static <T> AvroIO.Write<T> create(Class<T> clazz, String toPath, String tmpPath) {
    return create(clazz, toPath, tmpPath, CodecFactory.snappyCodec());
  }

  public static <T> AvroIO.Write<T> create(
      Class<T> clazz, String toPath, String tmpPath, CodecFactory codec) {
    return AvroIO.write(clazz)
        .to(toPath)
        .withSuffix(".avro")
        .withCodec(codec)
        .withTempDirectory(FileSystems.matchNewResource(tmpPath, true));
  }

  public static <T> AvroIO.Write<T> create(Class<T> clazz, String toPath, CodecFactory codec) {
    return AvroIO.write(clazz).to(toPath).withSuffix(".avro").withCodec(codec);
  }
}
