package org.gbif.pipelines.transforms.extension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.MeasurementOrFactInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
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

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_OR_FACT;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the Measurements_or_facts extension, reads an avro, writes an avro, maps from value to
 * keyValue and transforms form{@link ExtendedRecord} to {@link MeasurementOrFactRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/dwc/measurements_or_facts.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MeasurementOrFactTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();
  private static final String BASE_NAME = MEASUREMENT_OR_FACT.name().toLowerCase();

  /**
   * Checks if list contains {@link RecordType#MEASUREMENT_OR_FACT}, else returns empty {@link
   * PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(List<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, MEASUREMENT_OR_FACT));
  }

  /** Maps {@link MeasurementOrFactRecord} to key value, where key is {@link ImageRecord#getId} */
  public static MapElements<MeasurementOrFactRecord, KV<String, MeasurementOrFactRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, MeasurementOrFactRecord>>() {})
        .via((MeasurementOrFactRecord mfr) -> KV.of(mfr.getId(), mfr));
  }

  /**
   * Reads avro files from path, which contains {@link MeasurementOrFactRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<MeasurementOrFactRecord> read(String path) {
    return AvroIO.read(MeasurementOrFactRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link MeasurementOrFactRecord}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link MeasurementOrFactTransform#BASE_NAME}
   */
  public static AvroIO.Read<MeasurementOrFactRecord> read(UnaryOperator<String> pathFn) {
    return read(pathFn.apply(BASE_NAME));
  }

  /**
   * Writes {@link MeasurementOrFactRecord} *.avro files to path, data will be split into several files,
   * uses Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<MeasurementOrFactRecord> write(String toPath) {
    return AvroIO.write(MeasurementOrFactRecord.class)
        .to(toPath)
        .withSuffix(Pipeline.AVRO_EXTENSION)
        .withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link MeasurementOrFactRecord} *.avro files to path, data will be split into several files,
   * uses Snappy compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link MeasurementOrFactTransform#BASE_NAME}
   */
  public static AvroIO.Write<MeasurementOrFactRecord> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(BASE_NAME));
  }

  /**
   * Creates an {@link Interpreter} for {@link MeasurementOrFactRecord}
   */
  public static SingleOutput<ExtendedRecord, MeasurementOrFactRecord> interpret() {
    return ParDo.of(new Interpreter());
  }

  /**
   * ParDo runs sequence of interpretations for {@link MeasurementOrFactRecord} using {@link
   * ExtendedRecord} as a source and {@link MeasurementOrFactInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, MeasurementOrFactRecord> {

    private final Counter counter = Metrics.counter(MeasurementOrFactTransform.class, MEASUREMENT_OR_FACT_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> MeasurementOrFactRecord.newBuilder().setId(er.getId()).setCreated(Instant.now().toEpochMilli()).build())
          .when(er -> Optional.ofNullable(er.getExtensions().get(Extension.MEASUREMENT_OR_FACT.getRowType()))
              .filter(l -> !l.isEmpty())
              .isPresent())
          .via(MeasurementOrFactInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }
}
