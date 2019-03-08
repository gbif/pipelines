package org.gbif.pipelines.transforms.extension;

import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.MultimediaInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
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

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MULTIMEDIA_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MULTIMEDIA;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the Multimedia extension, reads an avro, writes an avro, maps from value to keyValue
 * and transforms form{@link ExtendedRecord} to {@link MultimediaRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/gbif/1.0/multimedia.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MultimediaTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();
  private static final String BASE_NAME = MULTIMEDIA.name().toLowerCase();

  /**
   * Checks if list contains {@link RecordType#MULTIMEDIA}, else returns empty {@link PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(List<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, MULTIMEDIA));
  }

  /** Maps {@link MultimediaRecord} to key value, where key is {@link MultimediaRecord#getId} */
  public static MapElements<MultimediaRecord, KV<String, MultimediaRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, MultimediaRecord>>() {})
        .via((MultimediaRecord mr) -> KV.of(mr.getId(), mr));
  }

  /**
   * Reads avro files from path, which contains {@link MultimediaRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<MultimediaRecord> read(String path) {
    return AvroIO.read(MultimediaRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link MultimediaRecord}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link MultimediaTransform#BASE_NAME}
   */
  public static AvroIO.Read<MultimediaRecord> read(UnaryOperator<String> pathFn) {
    return read(pathFn.apply(BASE_NAME));
  }

  /**
   * Writes {@link MultimediaRecord} *.avro files to path, data will be split into several files,
   * uses Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<MultimediaRecord> write(String toPath) {
    return AvroIO.write(MultimediaRecord.class).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link MultimediaRecord} *.avro files to path, data will be split into several files,
   * uses Snappy compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link MultimediaTransform#BASE_NAME}
   */
  public static AvroIO.Write<MultimediaRecord> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(BASE_NAME));
  }

  /**
   * Creates an {@link Interpreter} for {@link MultimediaRecord}
   */
  public static SingleOutput<ExtendedRecord, MultimediaRecord> interpret() {
    return ParDo.of(new Interpreter());
  }

  /**
   * ParDo runs sequence of interpretations for {@link MultimediaRecord} using {@link
   * ExtendedRecord} as a source and {@link MultimediaInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, MultimediaRecord> {

    private final Counter counter = Metrics.counter(MultimediaTransform.class, MULTIMEDIA_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> MultimediaRecord.newBuilder().setId(er.getId()).build())
          .when(er -> Optional.ofNullable(er.getExtensions().get(Extension.MULTIMEDIA.getRowType()))
              .filter(l -> !l.isEmpty())
              .isPresent())
          .via(MultimediaInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }
}
