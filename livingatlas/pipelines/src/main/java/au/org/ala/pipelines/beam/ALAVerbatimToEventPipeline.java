package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.*;

import au.org.ala.kvs.ALANameMatchConfig;
import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.cache.ALANameCheckKVStoreFactory;
import au.org.ala.kvs.cache.ALANameMatchKVStoreFactory;
import au.org.ala.kvs.cache.GeocodeKvStoreFactory;
import au.org.ala.pipelines.transforms.*;
import au.org.ala.pipelines.transforms.LocationTransform;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.factory.FileVocabularyFactory;
import org.gbif.pipelines.io.avro.ALAMetadataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link ExtendedRecord} file to:
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
public class ALAVerbatimToEventPipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Event;

  public static void main(String[] args) throws IOException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "interpret");
    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.createInterpretation(combinedArgs);
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
    Set<String> types = getEventTypes(options.getInterpretationTypes());
    String targetPath = options.getTargetPath();

    MDC.put("datasetKey", datasetId);
    MDC.put("step", StepType.EVENTS_VERBATIM_TO_INTERPRETED.name());
    MDC.put("attempt", attempt.toString());

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    ALAPipelinesConfig config =
        FsUtils.readConfigFile(hdfsConfigs, options.getProperties(), ALAPipelinesConfig.class);

    List<DateComponentOrdering> dateComponentOrdering =
        options.getDefaultDateFormat() == null
            ? config.getGbifConfig().getDefaultDateFormat()
            : options.getDefaultDateFormat();

    FsUtils.deleteInterpretIfExist(hdfsConfigs, targetPath, datasetId, attempt, CORE_TERM, types);

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    // Path to write
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, id);

    log.info("Creating a pipeline from options");
    Pipeline p = pipelinesFn.apply(options);

    // Used transforms
    TransformsFactory transformsFactory = TransformsFactory.create(options);

    // Metadata
    ALAMetadataTransform metadataTransform =
        ALAMetadataTransform.builder()
            .dataResourceKvStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .datasetId(datasetId)
            .create();
    LocationTransform locationTransform =
        LocationTransform.builder()
            .alaConfig(config)
            .countryKvStoreSupplier(GeocodeKvStoreFactory.createCountrySupplier(config))
            .stateProvinceKvStoreSupplier(GeocodeKvStoreFactory.createStateProvinceSupplier(config))
            .biomeKvStoreSupplier(GeocodeKvStoreFactory.createBiomeSupplier(config))
            .create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
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
    ALATemporalTransform temporalTransform = ALATemporalTransform.builder().create();
    MultimediaTransform multimediaTransform = transformsFactory.createMultimediaTransform();
    AudubonTransform audubonTransform = transformsFactory.createAudubonTransform();
    ImageTransform imageTransform = transformsFactory.createImageTransform();
    EventCoreTransform eventCoreTransform =
        EventCoreTransform.builder()
            .vocabularyServiceSupplier(
                FileVocabularyFactory.builder()
                    .config(config.getGbifConfig())
                    .hdfsConfigs(hdfsConfigs)
                    .build()
                    .getInstanceSupplier())
            .create();
    IdentifierTransform identifierTransform = transformsFactory.createIdentifierTransform();
    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.builder().create();
    log.info("Creating beam pipeline");

    PCollection<ALAMetadataRecord> metadataRecord =
        p.apply("Create metadata collection", Create.of(options.getDatasetId()))
            .apply("Interpret metadata", metadataTransform.interpret());

    metadataRecord.apply("Write metadata to avro", metadataTransform.write(pathFn));

    // Read raw records and filter duplicates
    PCollection<ExtendedRecord> uniqueRawRecords =
        p.apply("Read event  verbatim", verbatimTransform.read(options.getInputPath()))
            .apply("Filter event duplicates", transformsFactory.createUniqueIdTransform())
            .apply("Filter event extensions", transformsFactory.createExtensionFilterTransform());

    // view with the records that have parents to find the hierarchy in the event core
    // interpretation later
    PCollectionView<Map<String, Map<String, String>>> erWithParentEventsView =
        uniqueRawRecords
            .apply(verbatimTransform.toParentEventsKv())
            .apply("View to find parents", View.asMap());
    eventCoreTransform.setErWithParentsView(erWithParentEventsView);

    uniqueRawRecords
        .apply("Interpret event identifiers", identifierTransform.interpret())
        .apply("Write event identifiers to avro", identifierTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check event core transform", eventCoreTransform.check(types))
        .apply("Interpret event core", eventCoreTransform.interpret())
        .apply("Write event core to avro", eventCoreTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check event temporal transform", temporalTransform.check(types))
        .apply("Interpret event temporal", temporalTransform.interpret())
        .apply("Write event temporal to avro", temporalTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check event taxonomy transform", alaTaxonomyTransform.check(types))
        .apply("Interpret event taxonomy", alaTaxonomyTransform.interpret())
        .apply("Write event taxon to avro", alaTaxonomyTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check event multimedia transform", multimediaTransform.check(types))
        .apply("Interpret event multimedia", multimediaTransform.interpret())
        .apply("Write event multimedia to avro", multimediaTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check event audubon transform", audubonTransform.check(types))
        .apply("Interpret event audubon", audubonTransform.interpret())
        .apply("Write event audubon to avro", audubonTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check event image transform", imageTransform.check(types))
        .apply("Interpret event image", imageTransform.interpret())
        .apply("Write event image to avro", imageTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check location transform", locationTransform.check(types))
        .apply("Interpret event location", locationTransform.interpret())
        .apply("Write event location to avro", locationTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check event measurementOrFact", measurementOrFactTransform.check(types))
        .apply("Interpret event measurementOrFact", measurementOrFactTransform.interpret())
        .apply("Write event measurementOrFact to avro", measurementOrFactTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check event verbatim transform", verbatimTransform.check(types))
        .apply("Write event verbatim to avro", verbatimTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Save metrics into the file and set files owner");
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", targetPath, datasetId, attempt.toString());
    FsUtils.deleteDirectoryByPrefix(hdfsConfigs, tempPath, ".temp-beam");

    log.info("Pipeline has been finished");
  }

  /** Remove directories with avro files for expected interpretation, except IDENTIFIER */
  private static Set<String> getEventTypes(Set<String> types) {
    Set<String> resultTypes = new HashSet<>(types);
    if (types.contains(BASIC.name())) {
      resultTypes.add(EVENT.name());
    }
    resultTypes.add(IDENTIFIER.name());
    resultTypes.add(EVENT_IDENTIFIER.name());
    resultTypes.remove(IDENTIFIER_ABSENT.name());
    return resultTypes;
  }

  private static boolean useMetadataRecordWriteIO(Set<String> types) {
    return types.contains(PipelinesVariables.Pipeline.Interpretation.RecordType.METADATA.name())
        || types.contains(PipelinesVariables.Pipeline.Interpretation.RecordType.ALL.name());
  }
}
