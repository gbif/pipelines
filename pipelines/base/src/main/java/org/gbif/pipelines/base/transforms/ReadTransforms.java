package org.gbif.pipelines.base.transforms;

import org.gbif.pipelines.base.options.base.BaseOptions;
import org.gbif.pipelines.base.utils.FsUtils;
import org.gbif.pipelines.core.RecordType;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.util.function.BiFunction;

import org.apache.beam.sdk.io.AvroIO;

import static org.gbif.pipelines.core.RecordType.BASIC;
import static org.gbif.pipelines.core.RecordType.LOCATION;
import static org.gbif.pipelines.core.RecordType.MULTIMEDIA;
import static org.gbif.pipelines.core.RecordType.TAXONOMY;
import static org.gbif.pipelines.core.RecordType.TEMPORAL;

/** TODO: DOC */
public class ReadTransforms {

  private ReadTransforms() {}

  private static BiFunction<BaseOptions, String, String> pathFn =
      (o, s) -> FsUtils.buildPath(o, s + "*.avro");
  private static BiFunction<BaseOptions, RecordType, String> pathInterFn =
      (o, t) -> FsUtils.buildPathInterpret(o, t.name().toLowerCase(), "*.avro");

  /** TODO: DOC */
  public static AvroIO.Read<BasicRecord> basic(BaseOptions options) {
    String path = pathInterFn.apply(options, BASIC);
    return AvroIO.read(BasicRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<TemporalRecord> temporal(BaseOptions options) {
    String path = pathInterFn.apply(options, TEMPORAL);
    return AvroIO.read(TemporalRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<TaxonRecord> taxon(BaseOptions options) {
    String path = pathInterFn.apply(options, TAXONOMY);
    return AvroIO.read(TaxonRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<LocationRecord> location(BaseOptions options) {
    String path = pathInterFn.apply(options, LOCATION);
    return AvroIO.read(LocationRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<MultimediaRecord> multimedia(BaseOptions options) {
    String path = pathInterFn.apply(options, MULTIMEDIA);
    return AvroIO.read(MultimediaRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<ExtendedRecord> verbatim(BaseOptions options) {
    String path = pathFn.apply(options, "verbatim");
    return AvroIO.read(ExtendedRecord.class).from(path);
  }

  /** TODO: DOC */
  public static AvroIO.Read<MetadataRecord> metadata(BaseOptions options) {
    String path = pathFn.apply(options, "metadata");
    return AvroIO.read(MetadataRecord.class).from(path);
  }
}
