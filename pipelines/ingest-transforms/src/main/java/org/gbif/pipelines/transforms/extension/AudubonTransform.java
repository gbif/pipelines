package org.gbif.pipelines.transforms.extension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.AudubonInterpreter;
import org.gbif.pipelines.io.avro.AudubonRecord;
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

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUDUBON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AUDUBON;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MULTIMEDIA;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the Audubon extension, reads an avro, writes an avro, maps from value to keyValue
 * and transforms form {@link ExtendedRecord} to {@link AudubonRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/ac/audubon.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AudubonTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();
  private static final String BASE_NAME = AUDUBON.name().toLowerCase();

  /**
   * Checks if list contains {@link RecordType#AUDUBON}, else returns empty {@link PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(List<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, AUDUBON, MULTIMEDIA));
  }

  /** Maps {@link AudubonRecord} to key value, where key is {@link AudubonRecord#getId} */
  public static MapElements<AudubonRecord, KV<String, AudubonRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, AudubonRecord>>() {})
        .via((AudubonRecord ar) -> KV.of(ar.getId(), ar));
  }

  /**
   * Reads avro files from path, which contains {@link AudubonRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<AudubonRecord> read(String path) {
    return AvroIO.read(AudubonRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link AudubonRecord}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link AudubonTransform#BASE_NAME}
   */
  public static AvroIO.Read<AudubonRecord> read(UnaryOperator<String> pathFn) {
    return read(pathFn.apply(BASE_NAME));
  }

  /**
   * Writes {@link AudubonRecord} *.avro files to path, data will be split into several files,
   * uses Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<AudubonRecord> write(String toPath) {
    return AvroIO.write(AudubonRecord.class).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link AudubonRecord} *.avro files to path, data will be split into several files,
   * uses Snappy compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link AudubonTransform#BASE_NAME}
   */
  public static AvroIO.Write<AudubonRecord> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(BASE_NAME));
  }

  /**
   * Creates an {@link Interpreter} for {@link AudubonRecord}
   */
  public static SingleOutput<ExtendedRecord, AudubonRecord> interpret() {
    return ParDo.of(new Interpreter());
  }

  /**
   * ParDo runs sequence of interpretations for {@link AudubonRecord} using {@link
   * ExtendedRecord} as a source and {@link AudubonInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, AudubonRecord> {

    private final Counter counter = Metrics.counter(AudubonTransform.class, AUDUBON_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> AudubonRecord.newBuilder().setId(er.getId()).setCreated(Instant.now().toEpochMilli()).build())
          .when(er -> Optional.ofNullable(er.getExtensions().get(Extension.AUDUBON.getRowType()))
              .filter(l -> !l.isEmpty())
              .isPresent())
          .via(AudubonInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }
}
