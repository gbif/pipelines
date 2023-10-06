package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.FRAGMENTER_COUNT;

import java.util.function.Function;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.io.hbase.HBaseIO.Write;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.fragmenter.common.HbaseStore;
import org.gbif.pipelines.fragmenter.common.RawRecord;
import org.gbif.pipelines.fragmenter.record.DwcaOccurrenceRecord;
import org.gbif.pipelines.fragmenter.record.OccurrenceRecordConverter;
import org.gbif.pipelines.ingest.pipelines.fragmenter.DwcaOccurrenceRecordIO;
import org.gbif.pipelines.ingest.pipelines.fragmenter.DwcaOccurrenceRecordIO.Read;
import org.gbif.pipelines.ingest.pipelines.fragmenter.RawRecordCoder;
import org.gbif.pipelines.ingest.pipelines.interpretation.TransformsFactory;
import org.gbif.pipelines.keygen.HBaseLockingKey;
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

    log.info("Adding step 0: Running Beam pipeline");
    String datasetKey = options.getDatasetId();
    String attempt = options.getAttempt().toString();

    MDC.put("datasetKey", datasetKey);
    MDC.put("attempt", attempt);
    MDC.put("step", StepType.FRAGMENTER.name());

    log.info("Adding step 1: Creating transforms functions");
    TransformsFactory transformsFactory = TransformsFactory.create(options);
    PipelinesConfig config = transformsFactory.getConfig();

    Read dwcaOccurrenceRecordReader =
        DwcaOccurrenceRecordIO.Read.fromLocation(options.getInputPath());

    RawRecordFn rawRecordFn =
        RawRecordFn.builder()
            .useTriplet(options.isTripletValid())
            .useOccurrenceId(options.isOccurrenceIdValid())
            .generateIdIfAbsent(options.getGenerateIds())
            .keygenServiceSupplier(transformsFactory.createHBaseLockingKeySupplier())
            .create();

    RawRecordFilterFn rawRecordFilterFn =
        RawRecordFilterFn.builder()
            .tableSupplier(transformsFactory.createFragmenterTableSupplier())
            .create();

    MutationConverterFn mutationConverterFn =
        MutationConverterFn.builder()
            .datasetKey(datasetKey)
            .attempt(Integer.valueOf(attempt))
            .protocol(EndpointType.DWC_ARCHIVE.name())
            .create();

    Write hbaseWrite =
        HBaseIO.write()
            .withConfiguration(getHBaseConfig(config))
            .withTableId(config.getFragmentsTable());

    log.info("Adding step 2: Creating pipeline steps");
    Pipeline p = pipelinesFn.apply(options);

    p.apply("Read from Darwin Core Archive", dwcaOccurrenceRecordReader)
        .apply("Convert to RawRecord", ParDo.of(rawRecordFn))
        .setCoder(RawRecordCoder.of())
        .apply("Filter RawRecord", ParDo.of(rawRecordFilterFn))
        .apply("Convert to Mutation", ParDo.of(mutationConverterFn))
        .apply("Push data into HBase", hbaseWrite);

    log.info("Adding step 3: Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
    log.info("Pipeline has been finished");
  }

  private static Configuration getHBaseConfig(PipelinesConfig config) {
    Configuration hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set("hbase.zookeeper.quorum", config.getZkConnectionString());
    hbaseConfig.set("zookeeper.session.timeout", "360000");
    return hbaseConfig;
  }

  private static class RawRecordFn extends DoFn<DwcaOccurrenceRecord, RawRecord> {
    private final boolean useTriplet;
    private final boolean useOccurrenceId;
    private final boolean generateIdIfAbsent;
    private final SerializableSupplier<HBaseLockingKey> keygenServiceSupplier;
    private HBaseLockingKey keygenService;

    @Builder(buildMethodName = "create")
    private RawRecordFn(
        boolean useTriplet,
        boolean useOccurrenceId,
        boolean generateIdIfAbsent,
        SerializableSupplier<HBaseLockingKey> keygenServiceSupplier) {
      this.useTriplet = useTriplet;
      this.useOccurrenceId = useOccurrenceId;
      this.generateIdIfAbsent = generateIdIfAbsent; // for IT tests
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
              keygenService, emptyValidator, useTriplet, useOccurrenceId, generateIdIfAbsent, dor)
          .ifPresent(out::output);
    }
  }

  private static class RawRecordFilterFn extends DoFn<RawRecord, RawRecord> {

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
        HbaseStore.populateCreatedDate(table, rr);
        out.output(rr);
      }
    }
  }

  @Builder(buildMethodName = "create")
  private static class MutationConverterFn extends DoFn<RawRecord, Mutation> {

    private final Counter counter = Metrics.counter(MutationConverterFn.class, FRAGMENTER_COUNT);

    String datasetKey;
    Integer attempt;
    String protocol;

    @ProcessElement
    public void processElement(@Element RawRecord rr, OutputReceiver<Mutation> out) {
      Put fragmentPut = HbaseStore.createFragmentPut(datasetKey, attempt, protocol, rr);

      out.output(fragmentPut);

      counter.inc();
    }
  }
}
