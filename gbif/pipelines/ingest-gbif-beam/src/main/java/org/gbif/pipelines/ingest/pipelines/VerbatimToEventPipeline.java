package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.BASIC;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EVENT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EVENT_IDENTIFIER;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER_ABSENT;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
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
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.ingest.pipelines.interpretation.TransformsFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.GbifEventIdTransform;
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
public class VerbatimToEventPipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Event;

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
    Set<String> types = getEventTypes(options.getInterpretationTypes());
    String targetPath = options.getTargetPath();

    MDC.put("datasetKey", datasetId);
    MDC.put("step", StepType.EVENTS_VERBATIM_TO_INTERPRETED.name());
    MDC.put("attempt", attempt.toString());

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    TransformsFactory transformsFactory = TransformsFactory.create(options);

    FsUtils.deleteInterpretIfExist(hdfsConfigs, targetPath, datasetId, attempt, CORE_TERM, types);

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    // Path to write
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, id);

    // Path function for reading avro files
    UnaryOperator<String> interpretedPathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, ALL_AVRO);

    log.info("Creating a pipeline from options");
    Pipeline p = pipelinesFn.apply(options);

    // Used transforms
    MetadataTransform metadataTransform = transformsFactory.createMetadataTransform();
    LocationTransform locationTransform = transformsFactory.createLocationTransform();
    VerbatimTransform verbatimTransform = transformsFactory.createVerbatimTransform();
    TemporalTransform temporalTransform = transformsFactory.createTemporalTransform();
    TaxonomyTransform taxonomyTransform = transformsFactory.createTaxonomyTransform();
    MultimediaTransform multimediaTransform = transformsFactory.createMultimediaTransform();
    AudubonTransform audubonTransform = transformsFactory.createAudubonTransform();
    ImageTransform imageTransform = transformsFactory.createImageTransform();
    EventCoreTransform eventCoreTransform = transformsFactory.createEventCoreTransform();
    IdentifierTransform identifierTransform = transformsFactory.createIdentifierTransform();
    MeasurementOrFactTransform measurementOrFactTransform =
        transformsFactory.createMeasurementOrFactTransform();
    GbifEventIdTransform idTransform = GbifEventIdTransform.builder().create();

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

    PCollectionView<MetadataRecord> metadataView =
        metadataRecord.apply("Convert to event metadata view", View.asSingleton());

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
        .apply("Check event taxonomy transform", taxonomyTransform.check(types))
        .apply("Interpret event taxonomy", taxonomyTransform.interpret())
        .apply("Write event taxon to avro", taxonomyTransform.write(pathFn));

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
        .apply("Interpret event location", locationTransform.interpret(metadataView))
        .apply("Write event location to avro", locationTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check event measurementOrFact", measurementOrFactTransform.check(types))
        .apply("Interpret event measurementOrFact", measurementOrFactTransform.interpret())
        .apply("Write event measurementOrFact to avro", measurementOrFactTransform.write(pathFn));

    uniqueRawRecords
        .apply("Check event verbatim transform", verbatimTransform.check(types))
        .apply("Write event verbatim to avro", verbatimTransform.write(pathFn));

    uniqueRawRecords
        .apply("Generate GbifIdRecords for events", idTransform.interpret())
        .apply("Write Gbif Event Id Records", idTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Save metrics into the file and set files owner");
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
    FsUtils.setOwnerToCrap(hdfsConfigs, metadataPath);

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", targetPath, datasetId, attempt.toString());
    FsUtils.deleteDirectoryByPrefix(hdfsConfigs, tempPath, ".temp-beam");

    log.info("Set interpreted files permissions");
    String interpretedPath =
        PathBuilder.buildDatasetAttemptPath(options, CORE_TERM.simpleName(), false);
    FsUtils.setOwnerToCrap(hdfsConfigs, interpretedPath);

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
    return types.contains(RecordType.METADATA.name()) || types.contains(RecordType.ALL.name());
  }
}
