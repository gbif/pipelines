package org.gbif.pipelines.transforms;

import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;

/** Set of different write functions */
public class WriteTransforms {

  private WriteTransforms() {}

  /**
   * Writes {@link MetadataRecord} *.avro files to path, without splitting the file, uses Snappy
   * codec compression by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<MetadataRecord> metadata(String toPath) {
    return create(MetadataRecord.class, toPath).withoutSharding();
  }

  /**
   * Writes {@link BasicRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<BasicRecord> basic(String toPath) {
    return create(BasicRecord.class, toPath);
  }

  /**
   * Writes {@link TemporalRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<TemporalRecord> temporal(String toPath) {
    return create(TemporalRecord.class, toPath);
  }

  /**
   * Writes {@link TaxonRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<TaxonRecord> taxon(String toPath) {
    return create(TaxonRecord.class, toPath);
  }

  /**
   * Writes {@link MultimediaRecord} *.avro files to path, data will be split into several files,
   * uses Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<MultimediaRecord> multimedia(String toPath) {
    return create(MultimediaRecord.class, toPath);
  }

  /**
   * Writes {@link LocationRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<LocationRecord> location(String toPath) {
    return create(LocationRecord.class, toPath);
  }

  /**
   * Writes {@link ExtendedRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<ExtendedRecord> extended(String toPath) {
    return create(ExtendedRecord.class, toPath);
  }

  /**
   * Writes {@link ExtendedRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static <T> AvroIO.Write<T> create(Class<T> clazz, String toPath) {
    return create(clazz, toPath, CodecFactory.snappyCodec());
  }

  /**
   * Writes Avro based classes data to path, uses Snappy compression codec by default
   *
   * @param clazz Avro based class
   * @param toPath path with name to output files, like - directory/name
   * @param tmpPath path to temporal files
   */
  public static <T> AvroIO.Write<T> create(Class<T> clazz, String toPath, String tmpPath) {
    return create(clazz, toPath, tmpPath, CodecFactory.snappyCodec());
  }

  /**
   * Writes Avro based class data to path
   *
   * @param clazz Avro based class
   * @param toPath path with name to output files, like - directory/name
   * @param tmpPath path to temporal files
   * @param codec Avro compression codec
   */
  public static <T> AvroIO.Write<T> create(
      Class<T> clazz, String toPath, String tmpPath, CodecFactory codec) {
    return create(clazz, toPath, codec)
        .withTempDirectory(FileSystems.matchNewResource(tmpPath, true));
  }

  /**
   * Writes Avro based class data to path
   *
   * @param clazz Avro based class
   * @param toPath path with name to output files, like - directory/name
   * @param codec Avro compression codec
   */
  public static <T> AvroIO.Write<T> create(Class<T> clazz, String toPath, CodecFactory codec) {
    return AvroIO.write(clazz).to(toPath).withSuffix(".avro").withCodec(codec);
  }
}
