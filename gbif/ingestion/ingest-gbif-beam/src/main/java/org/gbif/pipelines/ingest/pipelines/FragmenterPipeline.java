package org.gbif.pipelines.ingest.pipelines;

import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.io.hbase.HBaseIO.Write;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.ingest.pipelines.fragmenter.DwcaOccurrenceRecordConverterFn;
import org.gbif.pipelines.ingest.pipelines.fragmenter.MutationConverterFn;
import org.gbif.pipelines.ingest.pipelines.fragmenter.RawRecordCoder;
import org.gbif.pipelines.ingest.pipelines.fragmenter.RawRecordFilterFn;
import org.gbif.pipelines.ingest.pipelines.fragmenter.RawRecordFn;
import org.gbif.pipelines.ingest.pipelines.interpretation.TransformsFactory;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
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

    String verbatimAvroFile = Conversion.FILE_NAME + PipelinesVariables.Pipeline.AVRO_EXTENSION;
    String pathToAvro = PathBuilder.buildDatasetAttemptPath(options, verbatimAvroFile, true);

    VerbatimTransform verbatimTransform = transformsFactory.createVerbatimTransform();

    DwcaOccurrenceRecordConverterFn dwcaOccurrenceRecordConverterFn =
        DwcaOccurrenceRecordConverterFn.create();

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

    p.apply("Read verbatim", verbatimTransform.read(pathToAvro))
        .apply(
            "Read occurrences from extension",
            transformsFactory.createOccurrenceExtensionTransform())
        .apply("Convert to DwcaOccurrenceRecord", ParDo.of(dwcaOccurrenceRecordConverterFn))
        .apply("Get keys and convert to RawRecord", ParDo.of(rawRecordFn))
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
}
