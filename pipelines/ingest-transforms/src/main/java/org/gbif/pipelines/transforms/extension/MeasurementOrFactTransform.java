package org.gbif.pipelines.transforms.extension;

import java.util.List;
import java.util.Optional;

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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMEN_OR_FACT_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_OR_FACT;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

public class MeasurementOrFactTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  private MeasurementOrFactTransform() {}

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
   * ParDo runs sequence of interpretations for {@link MeasurementOrFactRecord} using {@link
   * ExtendedRecord} as a source and {@link MeasurementOrFactInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, MeasurementOrFactRecord> {

    private final Counter counter = Metrics.counter(MeasurementOrFactTransform.class, MEASUREMEN_OR_FACT_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> MeasurementOrFactRecord.newBuilder().setId(er.getId()).build())
          .when(er -> Optional.ofNullable(er.getExtensions().get(Extension.MEASUREMENT_OR_FACT.getRowType()))
              .filter(l -> !l.isEmpty())
              .isPresent())
          .via(MeasurementOrFactInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }
}
