package org.gbif.pipelines.ingest.pipelines;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.cache.GeocodeKvStoreFactory;
import au.org.ala.pipelines.transforms.ALATemporalTransform;
import au.org.ala.pipelines.transforms.LocationTransform;
import au.org.ala.pipelines.transforms.MetadataTransform;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.common.ExtensionFilterTransform;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link ExtendedRecord} file to:
 *      {@link org.gbif.pipelines.io.avro.IdentifierRecord},
 *      {@link org.gbif.pipelines.io.avro.EventCoreRecord},
 *      {@link ExtendedRecord}
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/examples-pipelines-BUILD_VERSION-shaded.jar
 * --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 * --attempt=1
 * --runner=SparkRunner
 * --targetPath=/some/path/to/output/
 * --inputPath=/some/path/to/output/0057a720-17c9-4658-971e-9578f3577cf5/1/verbatim.avro
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAVerbatimToInterpretedPipeline {

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

    ALAPipelinesConfig config =
        FsUtils.readConfigFile(
            hdfsSiteConfig, coreSiteConfig, options.getProperties(), ALAPipelinesConfig.class);

    List<DateComponentOrdering> dateComponentOrdering =
        options.getDefaultDateFormat() == null
            ? config.getGbifConfig().getDefaultDateFormat()
            : options.getDefaultDateFormat();

    FsUtils.deleteInterpretIfExist(
        hdfsSiteConfig, coreSiteConfig, targetPath, datasetId, attempt, types);

    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt.toString());

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, id);

    log.info("Creating a pipeline from options");
    Pipeline p = pipelinesFn.apply(options);

    // Used transforms
    // Metadata
    MetadataTransform metadataTransform =
        MetadataTransform.builder()
            .dataResourceKvStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .datasetId(datasetId)
            .create();

    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();
    IdentifierTransform identifierTransform =
        IdentifierTransform.builder().datasetKey(datasetId).create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    // TODO START: ALA uses the same LocationRecord but another Transform
    LocationTransform locationTransform =
        LocationTransform.builder()
            .alaConfig(config)
            .countryKvStoreSupplier(GeocodeKvStoreFactory.createCountrySupplier(config))
            .stateProvinceKvStoreSupplier(GeocodeKvStoreFactory.createStateProvinceSupplier(config))
            .biomeKvStoreSupplier(GeocodeKvStoreFactory.createBiomeSupplier(config))
            .create();

    // TODO END: ALA uses the same LocationRecord but another Transform
    ALATemporalTransform temporalTransform =
        ALATemporalTransform.builder().orderings(dateComponentOrdering).create();

    // Extension
    MultimediaTransform multimediaTransform =
        MultimediaTransform.builder().orderings(dateComponentOrdering).create();
    AudubonTransform audubonTransform =
        AudubonTransform.builder().orderings(dateComponentOrdering).create();
    ImageTransform imageTransform =
        ImageTransform.builder().orderings(dateComponentOrdering).create();

    log.info("Creating beam pipeline");
    // Metadata TODO START: Will ALA use it?
    PCollection<MetadataRecord> metadataRecord =
        p.apply("Create metadata collection", Create.of(options.getDatasetId()))
            .apply("Interpret metadata", metadataTransform.interpret());

    metadataRecord.apply("Write metadata to avro", metadataTransform.write(pathFn));
    // TODO END: Will ALA use it?

    // Read raw records and filter duplicates
    PCollection<ExtendedRecord> uniqueRawRecords =
        p.apply("Read verbatim", verbatimTransform.read(options.getInputPath()))
            .apply("Filter duplicates", UniqueIdTransform.create())
            .apply(
                "Filter extension",
                ExtensionFilterTransform.create(
                    config.getGbifConfig().getExtensionsAllowedForVerbatimSet()));

    // Interpret identifiers and wite as avro files
    uniqueRawRecords
        .apply("Interpret identifiers", identifierTransform.interpret())
        .apply("Write identifiers to avro", identifierTransform.write(pathFn));

    // Interpret event core records and wite as avro files
    uniqueRawRecords
        .apply("Interpret event core", eventCoreTransform.interpret())
        .apply("Write event to avro", eventCoreTransform.write(pathFn));

    uniqueRawRecords
        .apply("Interpret temporal", temporalTransform.interpret())
        .apply("Write temporal to avro", temporalTransform.write(pathFn));

    // TODO START: DO WE NEED ALL MULTIMEDIA EXTENSIONS?
    uniqueRawRecords
        .apply("Interpret multimedia", multimediaTransform.interpret())
        .apply("Write multimedia to avro", multimediaTransform.write(pathFn));

    uniqueRawRecords
        .apply("Interpret audubon", audubonTransform.interpret())
        .apply("Write audubon to avro", audubonTransform.write(pathFn));

    uniqueRawRecords
        .apply("Interpret image", imageTransform.interpret())
        .apply("Write image to avro", imageTransform.write(pathFn));
    // TODO END

    uniqueRawRecords
        .apply("Interpret location", locationTransform.interpret())
        .apply("Write location to avro", locationTransform.write(pathFn));

    // Wite filtered verbatim avro files
    uniqueRawRecords.apply("Write verbatim to avro", verbatimTransform.write(pathFn));

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
