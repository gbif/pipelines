package org.gbif.pipelines.transforms.specific;

import java.util.List;
import java.util.Optional;

import org.gbif.api.vocabulary.Country;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.AustraliaSpatialInterpreter;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.transforms.CheckTransforms;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUSTRALIA_SPATIAL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AUSTRALIA_SPATIAL;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the Australia location, read an avro, write an avro, from value to keyValue and
 * transforms form {@link LocationRecord} to {@link AustraliaSpatialRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#occurrence</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AustraliaSpatialTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  /**
   * Checks if list contains {@link RecordType#AUSTRALIA_SPATIAL}, else returns empty {@link
   * PCollection<LocationRecord>}
   */
  public static CheckTransforms<LocationRecord> check(List<String> types) {
    return CheckTransforms.create(LocationRecord.class, checkRecordType(types, AUSTRALIA_SPATIAL));
  }

  /** Maps {@link AustraliaSpatialRecord} to key value, where key is {@link AustraliaSpatialRecord#getId} */
  public static MapElements<AustraliaSpatialRecord, KV<String, AustraliaSpatialRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, AustraliaSpatialRecord>>() {})
        .via((AustraliaSpatialRecord asr) -> KV.of(asr.getId(), asr));
  }

  /**
   * Reads avro files from path, which contains {@link AustraliaSpatialRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<AustraliaSpatialRecord> read(String path) {
    return AvroIO.read(AustraliaSpatialRecord.class).from(path);
  }

  /**
   * Writes {@link AustraliaSpatialRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<AustraliaSpatialRecord> write(String toPath) {
    return AvroIO.write(AustraliaSpatialRecord.class)
        .to(toPath)
        .withSuffix(Pipeline.AVRO_EXTENSION)
        .withCodec(BASE_CODEC);
  }

  /**
   * Creates an {@link Interpreter} for {@link AustraliaSpatialRecord}
   */
  public static SingleOutput<LocationRecord, AustraliaSpatialRecord> interpret() {
    return ParDo.of(new Interpreter());
  }

  /**
   * ParDo runs sequence of interpretations for {@link AustraliaSpatialRecord} using {@link LocationRecord} as
   * a source and {@link AustraliaSpatialInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<LocationRecord, AustraliaSpatialRecord> {

    private final Counter counter = Metrics.counter(AustraliaSpatialTransform.class, AUSTRALIA_SPATIAL_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(lr -> AustraliaSpatialRecord.newBuilder().setId(lr.getId()).build())
          .when(lr -> Optional.ofNullable(lr.getCountry())
              .filter(c -> c.equals(Country.AUSTRALIA.getTitle()))
              .isPresent())
          .via(AustraliaSpatialInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }

}
