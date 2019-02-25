package org.gbif.pipelines.transforms.extension;

import java.util.List;

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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUDUBON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AUDUBON;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

public class AudubonTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  private AudubonTransform() {
  }

  /**
   * Checks if list contains {@link RecordType#AUDUBON}, else returns empty {@link
   * PCollection <ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(List<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, AUDUBON));
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
   * Writes {@link AudubonRecord} *.avro files to path, data will be split into several files,
   * uses Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<AudubonRecord> write(String toPath) {
    return AvroIO.write(AudubonRecord.class).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
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
          .to(er -> AudubonRecord.newBuilder().setId(er.getId()).build())
          .when(er -> er.getExtensions().containsKey(Extension.AUDUBON.getRowType()))
          .via(AudubonInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }
}
