package au.org.ala.pipelines.beam;

import au.org.ala.kvs.ALANameMatchConfig;
import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.cache.ALACollectionKVStoreFactory;
import au.org.ala.kvs.cache.ALANameCheckKVStoreFactory;
import au.org.ala.kvs.cache.ALANameMatchKVStoreFactory;
import au.org.ala.kvs.cache.GeocodeKvStoreFactory;
import au.org.ala.kvs.cache.RecordedByKVStoreFactory;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.transforms.ALAAttributionTransform;
import au.org.ala.pipelines.transforms.ALABasicTransform;
import au.org.ala.pipelines.transforms.ALADefaultValuesTransform;
import au.org.ala.pipelines.transforms.ALAMetadataTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.transforms.ALATemporalTransform;
import au.org.ala.pipelines.transforms.LocationTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ArchiveUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.factory.FileVocabularyFactory;
import org.gbif.pipelines.factory.OccurrenceStatusKvStoreFactory;
import org.gbif.pipelines.io.avro.ALAMetadataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.converters.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.DefaultValuesTransform;
import org.slf4j.MDC;

/**
 * Interpretation pipeline that reads verbatim AVRO files ({@link ExtendedRecord} and parses and
 * produces an interpreted output of these data.
 *
 * <p>The processing sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link ExtendedRecord} file to:
 *      {@link MetadataRecord},
 *      {@link DefaultValuesTransform},
 *      {@link ALABasicTransform},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *      {@link org.gbif.pipelines.io.avro.ImageRecord},
 *      {@link org.gbif.pipelines.io.avro.AudubonRecord},
 *      {@link org.gbif.pipelines.io.avro.ALATaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.ALAAttributionRecord},
 *      {@link LocationTransform}
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/pipelines-BUILD_VERSION-shaded.jar some.properties
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
 * --pipelineStep=VERBATIM_TO_ALA_INTERPRETED \
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
public class ALAVerbatimToInterpretedPipeline {
  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;

  public static void main(String[] args) throws IOException {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "interpret");
    ALAInterpretationPipelineOptions options =
        PipelinesOptionsFactory.create(ALAInterpretationPipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.INTERPRETATION_METRICS);
    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(ALAInterpretationPipelineOptions options) {

    log.info("Pipeline has been started - {}", LocalDateTime.now());
    boolean verbatimAvroAvailable = ValidationUtils.isVerbatimAvroAvailable(options);
    if (!verbatimAvroAvailable) {
      log.warn("Verbatim AVRO not available for {}", options.getDatasetId());
      return;
    }

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.VERBATIM_TO_INTERPRETED.name());

    Set<String> types = options.getInterpretationTypes();

    String targetPath = options.getTargetPath();
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    log.info("hdfsSiteConfig = " + hdfsConfigs.getHdfsSiteConfig());
    log.info("coreSiteConfig = " + hdfsConfigs.getCoreSiteConfig());

    FsUtils.deleteInterpretIfExist(hdfsConfigs, targetPath, datasetId, attempt, CORE_TERM, types);

    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(hdfsConfigs, options.getProperties()).get();

    List<DateComponentOrdering> dateComponentOrdering =
        options.getDefaultDateFormat() == null
            ? config.getGbifConfig().getDefaultDateFormat()
            : Arrays.asList(DateComponentOrdering.DMY_FORMATS);

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, id);

    log.info("Creating a pipeline from options");

    Pipeline p = Pipeline.create(options);

    ALACollectoryMetadata m =
        ALAAttributionKVStoreFactory.create(config).get(options.getDatasetId());
    if (Objects.equals(m, ALACollectoryMetadata.EMPTY)) {
      log.error(
          "Collectory metadata no available for {}. Will not run interpretation",
          options.getDatasetId());
      return;
    }

    ALAMetadataTransform metadataTransform =
        ALAMetadataTransform.builder()
            .dataResourceKvStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .datasetId(datasetId)
            .create();

    ALABasicTransform basicTransform =
        ALABasicTransform.builder()
            .vocabularyServiceSupplier(
                config.getGbifConfig().getVocabularyConfig() != null
                    ? FileVocabularyFactory.builder()
                        .config(config.getGbifConfig())
                        .hdfsConfigs(hdfsConfigs)
                        .build()
                        .getInstanceSupplier()
                    : null)
            .recordedByKvStoreSupplier(RecordedByKVStoreFactory.createSupplier(config))
            .occStatusKvStoreSupplier(
                OccurrenceStatusKvStoreFactory.createSupplier(config.getGbifConfig()))
            .create();

    VerbatimTransform verbatimTransform = VerbatimTransform.create();

    ALATemporalTransform temporalTransform =
        ALATemporalTransform.builder().orderings(dateComponentOrdering).create();

    // Extension
    MultimediaTransform multimediaTransform =
        MultimediaTransform.builder().orderings(dateComponentOrdering).create();

    // Extension
    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.builder().create();

    // Create and write metadata
    PCollection<ALAMetadataRecord> metadataRecord =
        p.apply("Create metadata collection", Create.of(options.getDatasetId()))
            .apply("Interpret metadata", metadataTransform.interpret());

    // Create View for the further usage
    PCollectionView<ALAMetadataRecord> metadataView =
        metadataRecord.apply("Convert into view", View.asSingleton());

    // ALA specific - Attribution
    ALAAttributionTransform alaAttributionTransform =
        ALAAttributionTransform.builder()
            .collectionKvStoreSupplier(ALACollectionKVStoreFactory.getInstanceSupplier(config))
            .metadataView(metadataView)
            .create();

    // ALA specific - Taxonomy
    ALATaxonomyTransform alaTaxonomyTransform =
        ALATaxonomyTransform.builder()
            .datasetId(datasetId)
            .nameMatchStoreSupplier(ALANameMatchKVStoreFactory.getInstanceSupplier(config))
            .kingdomCheckStoreSupplier(
                ALANameCheckKVStoreFactory.getInstanceSupplier("kingdom", config))
            .dataResourceStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .alaNameMatchConfig(
                config.getAlaNameMatchConfig() != null
                    ? config.getAlaNameMatchConfig()
                    : new ALANameMatchConfig())
            .create();

    // ALA specific - Location
    LocationTransform locationTransform =
        LocationTransform.builder()
            .alaConfig(config)
            .countryKvStoreSupplier(GeocodeKvStoreFactory.createCountrySupplier(config))
            .stateProvinceKvStoreSupplier(GeocodeKvStoreFactory.createStateProvinceSupplier(config))
            .biomeKvStoreSupplier(GeocodeKvStoreFactory.createBiomeSupplier(config))
            .create();

    // ALA specific - Default values
    ALADefaultValuesTransform alaDefaultValuesTransform =
        ALADefaultValuesTransform.builder()
            .datasetId(datasetId)
            .dataResourceKvStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .create();

    log.info("Creating beam pipeline");

    metadataRecord.apply("Write metadata to avro", metadataTransform.write(pathFn));

    alaAttributionTransform.setMetadataView(metadataView);

    // Interpret and write all record types
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", verbatimTransform.read(options.getInputPath()))
            .apply("Read occurrences from extension", OccurrenceExtensionTransform.create())
            .apply("Set default values", alaDefaultValuesTransform);

    uniqueRecords
        .apply("Check verbatim transform condition", verbatimTransform.check(types))
        .apply("Write verbatim to avro", verbatimTransform.write(pathFn));

    uniqueRecords
        .apply("Check basic transform condition", basicTransform.check(types))
        .apply("Interpret basic", basicTransform.interpret())
        .apply("Write basic to avro", basicTransform.write(pathFn));

    uniqueRecords
        .apply("Check temporal transform condition", temporalTransform.check(types))
        .apply("Interpret temporal", temporalTransform.interpret())
        .apply("Write temporal to avro", temporalTransform.write(pathFn));

    uniqueRecords
        .apply("Check multimedia transform condition", multimediaTransform.check(types))
        .apply("Interpret multimedia", multimediaTransform.interpret())
        .apply("Write multimedia to avro", multimediaTransform.write(pathFn));

    uniqueRecords
        .apply("Check collection attribution", alaAttributionTransform.check(types))
        .apply("Interpret ALA collection attribution", alaAttributionTransform.interpret())
        .apply("Write attribution to avro", alaAttributionTransform.write(pathFn));

    uniqueRecords
        .apply("Check ALA taxonomy transform condition", alaTaxonomyTransform.check(types))
        .apply("Interpret ALA taxonomy", alaTaxonomyTransform.interpret())
        .apply("Write ALA taxon to avro", alaTaxonomyTransform.write(pathFn));

    uniqueRecords
        .apply("Check location transform condition", locationTransform.check(types))
        .apply("Interpret location", locationTransform.interpret())
        .apply("Write location to avro", locationTransform.write(pathFn));

    uniqueRecords
        .apply("Check location transform condition", measurementOrFactTransform.check(types))
        .apply("Interpret location", measurementOrFactTransform.interpret())
        .apply("Write location to avro", measurementOrFactTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", targetPath, datasetId, attempt.toString());
    FsUtils.deleteDirectoryByPrefix(hdfsConfigs, tempPath, ".temp-beam");

    if (options.isEventsEnabled() && ArchiveUtils.isEventCore(options)) {
      log.info("Running events pipeline");
      ALAVerbatimToEventPipeline.run(options);
    }
    log.info("Pipeline has been finished");
  }
}
