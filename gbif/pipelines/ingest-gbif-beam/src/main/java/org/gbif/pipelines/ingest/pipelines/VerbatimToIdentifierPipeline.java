package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.DIRECTORY_NAME;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.factory.KeygenServiceFactory;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTupleTransform;
import org.slf4j.MDC;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimToIdentifierPipeline {

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      InterpretationPipelineOptions options,
      Function<InterpretationPipelineOptions, Pipeline> pipelinesFn) {

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    String targetPath = options.getTargetPath();
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    PipelinesConfig config =
        FsUtils.readConfigFile(hdfsConfigs, options.getProperties(), PipelinesConfig.class);

    options.setAppName("VERBATIM_TO_IDENTIFIER" + "_" + datasetId + "_" + attempt);
    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", "VERBATIM_TO_IDENTIFIER");

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, id);

    log.info("Creating a pipeline from options");
    Pipeline p = pipelinesFn.apply(options);

    // Core
    GbifIdTransform idTransform =
        GbifIdTransform.builder()
            .isTripletValid(options.isTripletValid())
            .isOccurrenceIdValid(options.isOccurrenceIdValid())
            .useExtendedRecordId(options.isUseExtendedRecordId())
            .generateIdIfAbsent(false)
            .keygenServiceSupplier(KeygenServiceFactory.createSupplier(config, datasetId))
            .create();

    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    GbifIdTupleTransform tupleTransform = GbifIdTupleTransform.create();

    FsUtils.deleteInterpretIfExist(
        hdfsConfigs, targetPath, datasetId, attempt, idTransform.getAllNames());

    log.info("Creating beam pipeline");

    PCollectionTuple idCollection =
        p.apply("Read ExtendedRecords", verbatimTransform.read(options.getInputPath()))
            .apply("Read occurrences from extension", OccurrenceExtensionTransform.create())
            .apply("Filter duplicates", UniqueIdTransform.create())
            .apply("Interpret GBIF ids", idTransform.interpret())
            .apply("Get tuple GBIF ids", tupleTransform);

    // Interpret and write all record types
    idCollection
        .get(tupleTransform.getTag())
        .apply("Write GBIF ids to avro", idTransform.write(pathFn));

    idCollection
        .get(tupleTransform.getAbsentTag())
        .apply(
            "Write absent GBIF ids to avro",
            idTransform.write(pathFn.apply(idTransform.getAbsentName())));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Save metrics into the file and set files owner");
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    if (!FsUtils.fileExists(hdfsConfigs, metadataPath)) {
      MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
      FsUtils.setOwner(hdfsConfigs, metadataPath, "crap", "supergroup");
    }

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", targetPath, datasetId, attempt.toString());
    FsUtils.deleteDirectoryByPrefix(hdfsConfigs, tempPath, ".temp-beam");

    log.info("Set interpreted files permissions");
    String interpretedPath = PathBuilder.buildDatasetAttemptPath(options, DIRECTORY_NAME, false);
    FsUtils.setOwner(hdfsConfigs, interpretedPath, "crap", "supergroup");

    log.info("Pipeline has been finished");
  }
}
