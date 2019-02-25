package org.gbif.pipelines.transforms.core;

import java.util.List;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;
import org.gbif.pipelines.io.avro.BasicRecord;
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

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.BASIC_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.BASIC;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

public class BasicTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  private BasicTransform() {}

  /**
   * Checks if list contains {@link RecordType#BASIC}, else returns empty {@link
   * PCollection <ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(List<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, BASIC));
  }

  /** Maps {@link BasicRecord} to key value, where key is {@link BasicRecord#getId} */
  public static MapElements<BasicRecord, KV<String, BasicRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, BasicRecord>>() {})
        .via((BasicRecord br) -> KV.of(br.getId(), br));
  }

  /**
   * Reads avro files from path, which contains {@link BasicRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<BasicRecord> read(String path) {
    return AvroIO.read(BasicRecord.class).from(path);
  }

  /**
   * Writes {@link BasicRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<BasicRecord> write(String toPath) {
    return AvroIO.write(BasicRecord.class).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * ParDo runs sequence of interpretations for {@link BasicRecord} using {@link ExtendedRecord} as
   * a source and {@link BasicInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, BasicRecord> {

    private final Counter counter = Metrics.counter(BasicTransform.class, BASIC_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> BasicRecord.newBuilder().setId(er.getId()).build())
          .via(BasicInterpreter::interpretBasisOfRecord)
          .via(BasicInterpreter::interpretSex)
          .via(BasicInterpreter::interpretEstablishmentMeans)
          .via(BasicInterpreter::interpretLifeStage)
          .via(BasicInterpreter::interpretTypeStatus)
          .via(BasicInterpreter::interpretIndividualCount)
          .via(BasicInterpreter::interpretReferences)
          .consume(context::output);

      counter.inc();
    }
  }

}
