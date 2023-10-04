package org.gbif.pipelines.ingest.pipelines;

import java.util.function.Function;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Table;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.DwcaIO.Read;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.fragmenter.common.HbaseStore;
import org.gbif.pipelines.fragmenter.common.RawRecord;
import org.gbif.pipelines.fragmenter.record.DwcaOccurrenceRecord;
import org.gbif.pipelines.fragmenter.record.DwcaOccurrenceRecordReader;
import org.gbif.pipelines.fragmenter.record.OccurrenceRecordConverter;
import org.gbif.pipelines.ingest.pipelines.interpretation.TransformsFactory;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.keygen.OccurrenceRecord;
import org.slf4j.MDC;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FragmenterPipeline {

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    run(options, Pipeline::create);
  }

  @SneakyThrows
  public static void run(
      InterpretationPipelineOptions options,
      Function<InterpretationPipelineOptions, Pipeline> pipelinesFn) {

    String datasetId = options.getDatasetId();
    String attempt = options.getAttempt().toString();

    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt);
    MDC.put("step", StepType.FRAGMENTER.name());

    TransformsFactory transformsFactory = TransformsFactory.create(options);

    Read<DwcaOccurrenceRecord> dwcaOccurrenceRecordReader =
        Read.create(
            DwcaOccurrenceRecord.class,
            DwcaOccurrenceRecordReader.fromLocation(options.getInputPath()));

    RawRecordFn rawRecordFn =
        RawRecordFn.builder()
            .useTriplet(options.isTripletValid())
            .useOccurrenceId(options.isOccurrenceIdValid())
            .keygenServiceSupplier(transformsFactory.createHBaseLockingKeySupplier())
            .create();

    RawRecordFilterFn rawRecordFilterFn =
        RawRecordFilterFn.builder()
            .tableSupplier(transformsFactory.createFragmenterTableSupplier())
            .create();

    log.info("Adding step 2: Pipeline steps");
    Pipeline p = pipelinesFn.apply(options);

    p.apply("Read from Darwin Core Archive", dwcaOccurrenceRecordReader)
        .apply("Convert to RawRecord", ParDo.of(rawRecordFn))
        .apply("Filter RawRecord", ParDo.of(rawRecordFilterFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
    log.info("Pipeline has been finished");
  }

  public static class RawRecordFn extends DoFn<OccurrenceRecord, RawRecord> {
    private final boolean useTriplet;
    private final boolean useOccurrenceId;
    private final SerializableSupplier<HBaseLockingKey> keygenServiceSupplier;
    private HBaseLockingKey keygenService;

    @Builder(buildMethodName = "create")
    private RawRecordFn(
        boolean useTriplet,
        boolean useOccurrenceId,
        SerializableSupplier<HBaseLockingKey> keygenServiceSupplier) {
      this.useTriplet = useTriplet;
      this.useOccurrenceId = useOccurrenceId;
      this.keygenServiceSupplier = keygenServiceSupplier;
    }

    /** Beam @Setup initializes resources */
    @Setup
    public void setup() {
      if (keygenService == null && keygenServiceSupplier != null) {
        keygenService = keygenServiceSupplier.get();
      }
    }

    /** Beam @Teardown closes initialized resources */
    @SneakyThrows
    @Teardown
    public void tearDown() {
      if (keygenService != null) {
        keygenService.close();
      }
    }

    @ProcessElement
    public void processElement(@Element DwcaOccurrenceRecord dor, OutputReceiver<RawRecord> out) {
      Predicate<String> emptyValidator = s -> true;
      OccurrenceRecordConverter.convert(
              keygenService, emptyValidator, useTriplet, useOccurrenceId, false, dor)
          .ifPresent(out::output);
    }
  }

  public static class RawRecordFilterFn extends DoFn<RawRecord, RawRecord> {

    private final SerializableSupplier<Table> tableSupplier;
    private Table table;

    @Builder(buildMethodName = "create")
    public RawRecordFilterFn(SerializableSupplier<Table> tableSupplier) {
      this.tableSupplier = tableSupplier;
    }

    @Setup
    public void setup() {
      if (table == null && tableSupplier != null) {
        table = tableSupplier.get();
      }
    }

    /** Beam @Teardown closes initialized resources */
    @SneakyThrows
    @Teardown
    public void tearDown() {
      if (table != null) {
        table.close();
      }
    }

    @ProcessElement
    public void processElement(@Element RawRecord rr, OutputReceiver<RawRecord> out) {
      boolean isNewRawRecord = HbaseStore.isNewRawRecord(table, rr);
      if (isNewRawRecord) {
        out.output(rr);
      }
    }
  }
}
