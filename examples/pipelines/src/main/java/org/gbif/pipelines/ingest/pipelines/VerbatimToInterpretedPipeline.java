package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.pojo.ErBrContainer;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.factory.MetadataServiceClientFactory;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.common.ExtensionFilterTransform;
import org.gbif.pipelines.transforms.common.FilterRecordsTransform;
import org.gbif.pipelines.transforms.common.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.metadata.DefaultValuesTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file to:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar some.properties
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
 * --pipelineStep=VERBATIM_TO_INTERPRETED \
 * --properties=/some/path/to/output/ws.properties
 * --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 * --attempt=1
 * --interpretationTypes=ALL
 * --runner=SparkRunner
 * --targetPath=/some/path/to/output/
 * --inputPath=/some/path/to/output/0057a720-17c9-4658-971e-9578f3577cf5/1/verbatim.avro
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimToInterpretedPipeline {

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
    Set<String> types = options.getInterpretationTypes();
    String targetPath = options.getTargetPath();
    String hdfsSiteConfig = options.getHdfsSiteConfig();
    String coreSiteConfig = options.getCoreSiteConfig();

    PipelinesConfig config =
        FsUtils.readConfigFile(
            hdfsSiteConfig, coreSiteConfig, options.getProperties(), PipelinesConfig.class);

    FsUtils.deleteInterpretIfExist(
        hdfsSiteConfig, coreSiteConfig, targetPath, datasetId, attempt, types);

    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.VERBATIM_TO_INTERPRETED.name());

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, id);

    UnaryOperator<String> interpretedPathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    log.info("Creating a pipeline from options");
    Pipeline p = pipelinesFn.apply(options);

    // Init external clients - ws, kv caches, etc
    SerializableSupplier<MetadataServiceClient> metadataServiceClientSupplier = null;
    if (options.getUseMetadataWsCalls()) {
      metadataServiceClientSupplier = MetadataServiceClientFactory.createSupplier(config);
    }

    // Core
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();

    VerbatimTransform verbatimTransform = VerbatimTransform.create();

    // Extra
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.create(options.isUseExtendedRecordId());

    log.info("Creating beam pipeline");

    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", verbatimTransform.read(options.getInputPath()))
            .apply("Filter duplicates", UniqueIdTransform.create())
            .apply(
                "Filter extension",
                ExtensionFilterTransform.create(config.getExtensionsAllowedForVerbatimSet()))
            .apply(
                "Set default values",
                DefaultValuesTransform.builder()
                    .clientSupplier(metadataServiceClientSupplier)
                    .datasetId(datasetId)
                    .create()
                    .interpret());

    // Filter record with identical GBIF ID
    PCollection<KV<String, ExtendedRecord>> uniqueRecordsKv =
        uniqueRecords.apply("Map verbatim to KV", verbatimTransform.toKv());

    // Process Basic record
    PCollectionTuple eventCoreCollection =
        uniqueRecords
            .apply("Interpret event core", eventCoreTransform.interpret())
            .apply("Get invalid GBIF IDs", gbifIdTransform);

    PCollection<KV<String, EventCoreRecord>> uniqueEventCoreRecordsKv =
        eventCoreCollection
            .get(gbifIdTransform.getTag())
            .apply("Map basic to KV", eventCoreTransform.toKv());

    // Interpret and write all record types
    eventCoreCollection
        .get(gbifIdTransform.getTag())
        .apply("Write basic to avro", eventCoreTransform.write(pathFn));

    eventCoreCollection
        .get(gbifIdTransform.getInvalidTag())
        .apply("Write invalid basic to avro", eventCoreTransform.writeInvalid(pathFn));

    FilterRecordsTransform filterRecordsTransform =
        FilterRecordsTransform.create(verbatimTransform.getTag(), eventCoreTransform.getTag());

    PCollection<ErBrContainer> filteredErBr =
        KeyedPCollectionTuple
            // Core
            .of(verbatimTransform.getTag(), uniqueRecordsKv)
            .and(eventCoreTransform.getTag(), uniqueEventCoreRecordsKv)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Filter verbatim", filterRecordsTransform.filter());

    PCollection<ExtendedRecord> filteredUniqueRecords =
        filteredErBr.apply(
            "Get the filtered extended records", filterRecordsTransform.extractExtendedRecords());

    filteredUniqueRecords
        .apply("Check verbatim transform condition", verbatimTransform.check(types))
        .apply("Write verbatim to avro", verbatimTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Save metrics into the file and set files owner");
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", targetPath, datasetId, attempt.toString());
    FsUtils.deleteDirectoryByPrefix(hdfsSiteConfig, coreSiteConfig, tempPath, ".temp-beam");

    log.info("Pipeline has been finished");
  }
}
