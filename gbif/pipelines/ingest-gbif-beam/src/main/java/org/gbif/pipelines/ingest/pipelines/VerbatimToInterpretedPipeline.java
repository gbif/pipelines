package org.gbif.pipelines.ingest.pipelines;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.factory.GeocodeKvStoreFactory;
import org.gbif.pipelines.factory.KeygenServiceFactory;
import org.gbif.pipelines.factory.MetadataServiceClientFactory;
import org.gbif.pipelines.factory.NameUsageMatchStoreFactory;
import org.gbif.pipelines.factory.OccurrenceStatusKvStoreFactory;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.common.FilterExtendedRecordTransform;
import org.gbif.pipelines.transforms.common.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.DefaultValuesTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.metadata.TaggedValuesTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file to:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *      {@link org.gbif.pipelines.io.avro.BasicRecord},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *      {@link org.gbif.pipelines.io.avro.ImageRecord},
 *      {@link org.gbif.pipelines.io.avro.AudubonRecord},
 *      {@link org.gbif.pipelines.io.avro.MeasurementOrFactRecord},
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord}
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

    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, id);

    log.info("Creating a pipeline from options");
    Pipeline p = Pipeline.create(options);

    // Metadata
    MetadataTransform metadataTransform =
        MetadataTransform.builder()
            .clientSupplier(MetadataServiceClientFactory.createSupplier(config))
            .attempt(attempt)
            .endpointType(options.getEndPointType())
            .create();

    TaggedValuesTransform taggedValuesTransform = TaggedValuesTransform.builder().create();

    // Core
    BasicTransform basicTransform =
        BasicTransform.builder()
            .keygenServiceSupplier(KeygenServiceFactory.createSupplier(config, datasetId))
            .occStatusKvStoreSupplier(OccurrenceStatusKvStoreFactory.createSupplier(config))
            .isTripletValid(options.isTripletValid())
            .isOccurrenceIdValid(options.isOccurrenceIdValid())
            .useExtendedRecordId(options.isUseExtendedRecordId())
            .create();

    VerbatimTransform verbatimTransform = VerbatimTransform.create();

    TemporalTransform temporalTransform = TemporalTransform.create();

    TaxonomyTransform taxonomyTransform =
        TaxonomyTransform.builder()
            .kvStoreSupplier(NameUsageMatchStoreFactory.createSupplier(config))
            .create();

    LocationTransform locationTransform =
        LocationTransform.builder()
            .geocodeKvStoreSupplier(GeocodeKvStoreFactory.createSupplier(config))
            .create();

    // Extension
    MeasurementOrFactTransform measurementOrFactTransform = MeasurementOrFactTransform.create();

    MultimediaTransform multimediaTransform = MultimediaTransform.create();

    AudubonTransform audubonTransform = AudubonTransform.create();

    ImageTransform imageTransform = ImageTransform.create();

    // Extra
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.create(options.isUseExtendedRecordId());

    log.info("Creating beam pipeline");
    // Create and write metadata
    PCollection<MetadataRecord> metadataRecord =
        p.apply("Create metadata collection", Create.of(options.getDatasetId()))
            .apply("Interpret metadata", metadataTransform.interpret());

    metadataRecord.apply("Write metadata to avro", metadataTransform.write(pathFn));

    // Create View for the further usage
    PCollectionView<MetadataRecord> metadataView =
        metadataRecord
            .apply("Check verbatim transform condition", metadataTransform.checkMetadata(types))
            .apply("Convert into view", View.asSingleton());

    locationTransform.setMetadataView(metadataView);
    taggedValuesTransform.setMetadataView(metadataView);

    PCollection<ExtendedRecord> uniqueRecords =
        metadataTransform.metadataOnly(types)
            ? verbatimTransform.emptyCollection(p)
            : p.apply("Read ExtendedRecords", verbatimTransform.read(options.getInputPath()))
                .apply("Read occurrences from extension", OccurrenceExtensionTransform.create())
                .apply("Filter duplicates", UniqueIdTransform.create())
                .apply(
                    "Set default values",
                    DefaultValuesTransform.builder()
                        .clientSupplier(MetadataServiceClientFactory.createSupplier(config))
                        .datasetId(datasetId)
                        .create());

    PCollectionTuple basicCollection =
        uniqueRecords
            .apply("Check basic transform condition", basicTransform.check(types))
            .apply("Interpret basic", basicTransform.interpret())
            .apply("Get invalid GBIF IDs", gbifIdTransform);

    // Filter record with identical GBIF ID
    PCollection<KV<String, ExtendedRecord>> uniqueRecordsKv =
        uniqueRecords.apply("Map verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, BasicRecord>> uniqueBasicRecordsKv =
        basicCollection
            .get(gbifIdTransform.getInvalidTag())
            .apply("Map basic to KV", basicTransform.toKv());

    SingleOutput<KV<String, CoGbkResult>, ExtendedRecord> filterByGbifIdFn =
        FilterExtendedRecordTransform.create(verbatimTransform.getTag(), basicTransform.getTag())
            .filter();

    PCollection<ExtendedRecord> filteredUniqueRecords =
        KeyedPCollectionTuple
            // Core
            .of(verbatimTransform.getTag(), uniqueRecordsKv)
            .and(basicTransform.getTag(), uniqueBasicRecordsKv)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Filter verbatim", filterByGbifIdFn);

    // Interpret and write all record types
    basicCollection
        .get(gbifIdTransform.getTag())
        .apply("Write basic to avro", basicTransform.write(pathFn));

    basicCollection
        .get(gbifIdTransform.getInvalidTag())
        .apply("Write invalid basic to avro", basicTransform.writeInvalid(pathFn));

    filteredUniqueRecords
        .apply("Check verbatim transform condition", verbatimTransform.check(types))
        .apply("Write verbatim to avro", verbatimTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check tagged values transform condition", taggedValuesTransform.check(types))
        .apply("Interpret tagged values", taggedValuesTransform.interpret())
        .apply("Write tagged values to avro", taggedValuesTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check temporal transform condition", temporalTransform.check(types))
        .apply("Interpret temporal", temporalTransform.interpret())
        .apply("Write temporal to avro", temporalTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check multimedia transform condition", multimediaTransform.check(types))
        .apply("Interpret multimedia", multimediaTransform.interpret())
        .apply("Write multimedia to avro", multimediaTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check image transform condition", imageTransform.check(types))
        .apply("Interpret image", imageTransform.interpret())
        .apply("Write image to avro", imageTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check audubon transform condition", audubonTransform.check(types))
        .apply("Interpret audubon", audubonTransform.interpret())
        .apply("Write audubon to avro", audubonTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check measurement transform condition", measurementOrFactTransform.check(types))
        .apply("Interpret measurement", measurementOrFactTransform.interpret())
        .apply("Write measurement to avro", measurementOrFactTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check taxonomy transform condition", taxonomyTransform.check(types))
        .apply("Interpret taxonomy", taxonomyTransform.interpret())
        .apply("Write taxon to avro", taxonomyTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check location transform condition", locationTransform.check(types))
        .apply("Interpret location", locationTransform.interpret())
        .apply("Write location to avro", locationTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", targetPath, datasetId, attempt.toString());
    FsUtils.deleteDirectoryByPrefix(hdfsSiteConfig, coreSiteConfig, tempPath, ".temp-beam");

    log.info("Pipeline has been finished");
  }
}
