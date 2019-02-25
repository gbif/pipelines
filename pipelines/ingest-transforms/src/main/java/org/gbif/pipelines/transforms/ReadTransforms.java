package org.gbif.pipelines.transforms;

import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.beam.sdk.io.AvroIO;

/** Set of different read functions */
public class ReadTransforms {

  private ReadTransforms() {}

  /**
   * Reads avro files from path, which contains {@link BasicRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<BasicRecord> basic(String path) {
    return AvroIO.read(BasicRecord.class).from(path);
  }

  /**
   * Readsavro files from path, which contains {@link TemporalRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<TemporalRecord> temporal(String path) {
    return AvroIO.read(TemporalRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link TaxonRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<TaxonRecord> taxon(String path) {
    return AvroIO.read(TaxonRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link LocationRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<LocationRecord> location(String path) {
    return AvroIO.read(LocationRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link ExtendedRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<ExtendedRecord> extended(String path) {
    return AvroIO.read(ExtendedRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link MetadataRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<MetadataRecord> metadata(String path) {
    return AvroIO.read(MetadataRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link MultimediaRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<MultimediaRecord> multimedia(String path) {
    return AvroIO.read(MultimediaRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link ImageRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<ImageRecord> image(String path) {
    return AvroIO.read(ImageRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link AudubonRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<AudubonRecord> audubon(String path) {
    return AvroIO.read(AudubonRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link MeasurementOrFactRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<MeasurementOrFactRecord> measurementOrFact(String path) {
    return AvroIO.read(MeasurementOrFactRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link AmplificationRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<AmplificationRecord> amplification(String path) {
    return AvroIO.read(AmplificationRecord.class).from(path);
  }
}
