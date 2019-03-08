package org.gbif.pipelines.transforms.extension;

import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.AmplificationInterpreter;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
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

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AMPLIFICATION_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AMPLIFICATION;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the Amplification extension, reads an avro, writes an avro, maps from value to
 * keyValue and transforms form {@link ExtendedRecord} to {@link AmplificationRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/ggbn/amplification.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AmplificationTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();
  private static final String BASE_NAME = AMPLIFICATION.name().toLowerCase();

  /**
   * Checks if list contains {@link RecordType#AMPLIFICATION}, else returns empty {@link PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(List<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, AMPLIFICATION));
  }

  /** Maps {@link AmplificationRecord} to key value, where key is {@link AmplificationRecord#getId} */
  public static MapElements<AmplificationRecord, KV<String, AmplificationRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, AmplificationRecord>>() {})
        .via((AmplificationRecord ar) -> KV.of(ar.getId(), ar));
  }

  /**
   * Reads avro files from path, which contains {@link AmplificationRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<AmplificationRecord> read(String path) {
    return AvroIO.read(AmplificationRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link AmplificationRecord}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link AmplificationTransform#BASE_NAME}
   */
  public static AvroIO.Read<AmplificationRecord> read(UnaryOperator<String> pathFn) {
    return read(pathFn.apply(BASE_NAME));
  }

  /**
   * Writes {@link AmplificationRecord} *.avro files to path, data will be split into several files, uses Snappy
   * AmplificationRecord codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<AmplificationRecord> write(String toPath) {
    return AvroIO.write(AmplificationRecord.class).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link AmplificationRecord} *.avro files to path, data will be split into several files, uses Snappy
   * AmplificationRecord codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link AmplificationTransform#BASE_NAME}
   */
  public static AvroIO.Write<AmplificationRecord> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(BASE_NAME));
  }

  /**
   * Creates an {@link Interpreter} for {@link AmplificationRecord}
   */
  public static SingleOutput<ExtendedRecord, AmplificationRecord> interpret() {
    return ParDo.of(new Interpreter());
  }

  /**
   * ParDo runs sequence of interpretations for {@link AmplificationRecord} using {@link
   * ExtendedRecord} as a source and {@link AmplificationInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, AmplificationRecord> {

    private final Counter counter = Metrics.counter(AmplificationTransform.class, AMPLIFICATION_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> AmplificationRecord.newBuilder().setId(er.getId()).build())
          .when(er -> Optional.ofNullable(er.getExtensions().get(AmplificationInterpreter.EXTENSION_ROW_TYPE))
              .filter(l -> !l.isEmpty())
              .isPresent())
          .via(AmplificationInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }
}
