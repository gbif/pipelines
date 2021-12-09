package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.DIRECTORY_NAME;

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
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.factory.FileVocabularyFactory;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.pojo.ErBrContainer;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.factory.ClusteringServiceFactory;
import org.gbif.pipelines.factory.GeocodeKvStoreFactory;
import org.gbif.pipelines.factory.GrscicollLookupKvStoreFactory;
import org.gbif.pipelines.factory.KeygenServiceFactory;
import org.gbif.pipelines.factory.MetadataServiceClientFactory;
import org.gbif.pipelines.factory.NameUsageMatchStoreFactory;
import org.gbif.pipelines.factory.OccurrenceStatusKvStoreFactory;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.common.CheckTransforms;
import org.gbif.pipelines.transforms.common.ExtensionFilterTransform;
import org.gbif.pipelines.transforms.common.FilterRecordsTransform;
import org.gbif.pipelines.transforms.common.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.DefaultValuesTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;
import org.gbif.rest.client.species.NameUsageMatch;
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
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord},
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

    List<DateComponentOrdering> dateComponentOrdering =
        options.getDefaultDateFormat() == null
            ? config.getDefaultDateFormat()
            : options.getDefaultDateFormat();

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
    SerializableSupplier<MetadataServiceClient> metadataServiceClientSerializableSupplier = null;
    if (options.getUseMetadataWsCalls()) {
      metadataServiceClientSerializableSupplier =
          MetadataServiceClientFactory.createSupplier(config);
    }
    SerializableSupplier<KeyValueStore<SpeciesMatchRequest, NameUsageMatch>>
        nameUsageMatchServiceSupplier = NameUsageMatchStoreFactory.createSupplier(config);
    SerializableSupplier<KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse>>
        grscicollServiceSupplier = GrscicollLookupKvStoreFactory.createSupplier(config);
    SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> geocodeServiceSupplier =
        GeocodeKvStoreFactory.createSupplier(config);
    if (options.getTestMode()) {
      metadataServiceClientSerializableSupplier = null;
      nameUsageMatchServiceSupplier = null;
      grscicollServiceSupplier = null;
      geocodeServiceSupplier = null;
    }

    // Metadata
    MetadataTransform metadataTransform =
        MetadataTransform.builder()
            .clientSupplier(metadataServiceClientSerializableSupplier)
            .attempt(attempt)
            .endpointType(options.getEndPointType())
            .create();

    // Core
    BasicTransform basicTransform =
        BasicTransform.builder()
            .useDynamicPropertiesInterpretation(true)
            .isTripletValid(options.isTripletValid())
            .isOccurrenceIdValid(options.isOccurrenceIdValid())
            .useExtendedRecordId(options.isUseExtendedRecordId())
            .occStatusKvStoreSupplier(OccurrenceStatusKvStoreFactory.createSupplier(config))
            .clusteringServiceSupplier(ClusteringServiceFactory.createSupplier(config))
            .keygenServiceSupplier(KeygenServiceFactory.createSupplier(config, datasetId))
            .fileVocabularyFactory(
                FileVocabularyFactory.builder()
                    .config(config)
                    .hdfsSiteConfig(hdfsSiteConfig)
                    .coreSiteConfig(coreSiteConfig)
                    .build())
            .create();

    VerbatimTransform verbatimTransform = VerbatimTransform.create();

    TemporalTransform temporalTransform =
        TemporalTransform.builder().orderings(dateComponentOrdering).create();

    TaxonomyTransform taxonomyTransform =
        TaxonomyTransform.builder().kvStoreSupplier(nameUsageMatchServiceSupplier).create();

    GrscicollTransform grscicollTransform =
        GrscicollTransform.builder().kvStoreSupplier(grscicollServiceSupplier).create();

    LocationTransform locationTransform =
        LocationTransform.builder().geocodeKvStoreSupplier(geocodeServiceSupplier).create();

    // Extension
    MultimediaTransform multimediaTransform =
        MultimediaTransform.builder().orderings(dateComponentOrdering).create();

    AudubonTransform audubonTransform =
        AudubonTransform.builder().orderings(dateComponentOrdering).create();

    ImageTransform imageTransform =
        ImageTransform.builder().orderings(dateComponentOrdering).create();

    // Extra
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.create(options.isUseExtendedRecordId());

    log.info("Creating beam pipeline");

    // Create and write metadata
    PCollection<MetadataRecord> metadataRecord;
    if (useMetadataRecordWriteIO(types)) {
      metadataRecord =
          p.apply("Create metadata collection", Create.of(options.getDatasetId()))
              .apply("Interpret metadata", metadataTransform.interpret());

      metadataRecord.apply("Write metadata to avro", metadataTransform.write(pathFn));
    } else {
      metadataRecord = p.apply("Read metadata record", metadataTransform.read(interpretedPathFn));
    }

    // Create View for the further usage
    PCollectionView<MetadataRecord> metadataView =
        metadataRecord.apply("Convert into view", View.asSingleton());

    locationTransform.setMetadataView(metadataView);
    grscicollTransform.setMetadataView(metadataView);

    PCollection<ExtendedRecord> uniqueRecords =
        metadataTransform.metadataOnly(types)
            ? verbatimTransform.emptyCollection(p)
            : p.apply("Read ExtendedRecords", verbatimTransform.read(options.getInputPath()))
                .apply("Read occurrences from extension", OccurrenceExtensionTransform.create())
                .apply("Filter duplicates", UniqueIdTransform.create())
                .apply(
                    "Filter extension",
                    ExtensionFilterTransform.create(config.getExtensionsAllowedForVerbatimSet()))
                .apply(
                    "Set default values",
                    DefaultValuesTransform.builder()
                        .clientSupplier(metadataServiceClientSerializableSupplier)
                        .datasetId(datasetId)
                        .create()
                        .interpret());

    // Filter record with identical GBIF ID
    PCollection<KV<String, ExtendedRecord>> uniqueRecordsKv =
        uniqueRecords.apply("Map verbatim to KV", verbatimTransform.toKv());

    // Process Basic record
    PCollection<KV<String, BasicRecord>> uniqueBasicRecordsKv;
    if (useBasicRecordWriteIO(types)) {
      PCollectionTuple basicCollection =
          uniqueRecords
              .apply("Interpret basic", basicTransform.interpret())
              .apply("Get invalid GBIF IDs", gbifIdTransform);

      uniqueBasicRecordsKv =
          basicCollection
              .get(gbifIdTransform.getTag())
              .apply("Map basic to KV", basicTransform.toKv());

      // Interpret and write all record types
      basicCollection
          .get(gbifIdTransform.getTag())
          .apply("Write basic to avro", basicTransform.write(pathFn));

      basicCollection
          .get(gbifIdTransform.getInvalidTag())
          .apply("Write invalid basic to avro", basicTransform.writeInvalid(pathFn));
    } else {
      uniqueBasicRecordsKv =
          p.apply("Read Basic records", basicTransform.read(interpretedPathFn))
              .apply("Map to Basic record KV", basicTransform.toKv());
    }

    FilterRecordsTransform filterRecordsTransform =
        FilterRecordsTransform.create(verbatimTransform.getTag(), basicTransform.getTag());

    PCollection<ErBrContainer> filteredErBr =
        KeyedPCollectionTuple
            // Core
            .of(verbatimTransform.getTag(), uniqueRecordsKv)
            .and(basicTransform.getTag(), uniqueBasicRecordsKv)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Filter verbatim", filterRecordsTransform.filter());

    PCollection<ExtendedRecord> filteredUniqueRecords =
        filteredErBr.apply(
            "Get the filtered extended records", filterRecordsTransform.extractExtendedRecords());

    filteredUniqueRecords
        .apply("Check verbatim transform condition", verbatimTransform.check(types))
        .apply("Write verbatim to avro", verbatimTransform.write(pathFn));

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
        .apply("Check taxonomy transform condition", taxonomyTransform.check(types))
        .apply("Interpret taxonomy", taxonomyTransform.interpret())
        .apply("Write taxon to avro", taxonomyTransform.write(pathFn));

    filteredErBr
        .apply(
            "Check grscicoll transform condition",
            grscicollTransform.check(types, ErBrContainer.class))
        .apply("Interpret grscicoll", grscicollTransform.interpret())
        .apply("Write grscicoll to avro", grscicollTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check location transform condition", locationTransform.check(types))
        .apply("Interpret location", locationTransform.interpret())
        .apply("Write location to avro", locationTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Save metrics into the file and set files owner");
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    if (!FsUtils.fileExists(hdfsSiteConfig, coreSiteConfig, metadataPath)
        || CheckTransforms.checkRecordType(types, RecordType.BASIC)
        || CheckTransforms.checkRecordType(types, RecordType.ALL)) {
      MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
      FsUtils.setOwner(hdfsSiteConfig, coreSiteConfig, metadataPath, "crap", "supergroup");
    }

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", targetPath, datasetId, attempt.toString());
    FsUtils.deleteDirectoryByPrefix(hdfsSiteConfig, coreSiteConfig, tempPath, ".temp-beam");

    log.info("Set interpreted files permissions");
    String interpretedPath = PathBuilder.buildDatasetAttemptPath(options, DIRECTORY_NAME, false);
    FsUtils.setOwner(hdfsSiteConfig, coreSiteConfig, interpretedPath, "crap", "supergroup");

    log.info("Pipeline has been finished");
  }

  private static boolean useBasicRecordWriteIO(Set<String> types) {
    return types.contains(RecordType.BASIC.name()) || types.contains(RecordType.ALL.name());
  }

  private static boolean useMetadataRecordWriteIO(Set<String> types) {
    return types.contains(RecordType.METADATA.name()) || types.contains(RecordType.ALL.name());
  }
}
