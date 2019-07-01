package org.gbif.pipelines.transforms.core;

import java.time.Instant;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
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

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TEMPORAL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TEMPORAL;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the DWC Event, reads an avro, writes an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link TemporalRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#event">https://dwc.tdwg.org/terms/#event</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemporalTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();
  private static final String BASE_NAME = TEMPORAL.name().toLowerCase();

  /**
   * Checks if list contains {@link RecordType#TEMPORAL}, else returns empty {@link PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(Set<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, TEMPORAL));
  }

  /** Maps {@link TemporalRecord} to key value, where key is {@link TemporalRecord#getId} */
  public static MapElements<TemporalRecord, KV<String, TemporalRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, TemporalRecord>>() {})
        .via((TemporalRecord tr) -> KV.of(tr.getId(), tr));
  }

  /**
   * Reads avro files from path, which contains {@link TemporalRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<TemporalRecord> read(String path) {
    return AvroIO.read(TemporalRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link TemporalRecord}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link TemporalTransform#BASE_NAME
   */
  public static AvroIO.Read<TemporalRecord> read(UnaryOperator<String> pathFn) {
    return read(pathFn.apply(BASE_NAME));
  }

  /**
   * Writes {@link TemporalRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<TemporalRecord> write(String toPath) {
    return AvroIO.write(TemporalRecord.class).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link TemporalRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link TemporalTransform#BASE_NAME}
   */
  public static AvroIO.Write<TemporalRecord> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(BASE_NAME));
  }

  /**
   * Creates an {@link Interpreter} for {@link TemporalRecord}
   */
  public static SingleOutput<ExtendedRecord, TemporalRecord> interpret() {
    return ParDo.of(new Interpreter());
  }

  /**
   * ParDo runs sequence of interpretations for {@link TemporalRecord} using {@link ExtendedRecord}
   * as a source and {@link TemporalInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, TemporalRecord> {

    private final Counter counter = Metrics.counter(TemporalTransform.class, TEMPORAL_RECORDS_COUNT);

    @ProcessElement
    public void processElement(@Element ExtendedRecord source, OutputReceiver<TemporalRecord> out) {

      TemporalRecord tr = TemporalRecord.newBuilder()
          .setId(source.getId())
          .setCreated(Instant.now().toEpochMilli())
          .build();

      Interpretation.from(source)
          .to(tr)
          .when(er -> !er.getCoreTerms().isEmpty())
          .via(TemporalInterpreter::interpretEventDate)
          .via(TemporalInterpreter::interpretDateIdentified)
          .via(TemporalInterpreter::interpretModifiedDate)
          .via(TemporalInterpreter::interpretDayOfYear);

      out.output(tr);

      counter.inc();
    }
  }
}
