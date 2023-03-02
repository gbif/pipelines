package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;
import static org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory.create;
import static org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory.registerHdfs;

import au.org.ala.pipelines.options.ALAEsIndexingPipelineOptions;
import au.org.ala.pipelines.transforms.ALADerivedMetadataTransform;
import au.org.ala.pipelines.transforms.ALAMetadataTransform;
import au.org.ala.pipelines.util.ElasticsearchTools;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;
import org.gbif.pipelines.transforms.common.NotNullOrEmptyFilter;
import org.gbif.pipelines.transforms.converters.ParentEventExpandTransform;
import org.gbif.pipelines.transforms.core.ConvexHullFn;
import org.gbif.pipelines.transforms.core.DerivedMetadataTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.InheritedFieldsTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TemporalCoverageFn;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
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
 *    1) Reads avro files:
 *      {@link EventCoreRecord},
 *      {@link IdentifierRecord},
 *      {@link ExtendedRecord},
 *    2) Joins avro files
 *    3) Converts to json model (resources/elasticsearch/es-event-core-schema.json)
 *    4) Pushes data to Elasticsearch instance
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/examples-pipelines-BUILD_VERSION-shaded.jar
 *  --pipelineStep=INTERPRETED_TO_ES_INDEX \
 *  --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 *  --attempt=1 \
 *  --runner=SparkRunner \
 *  --inputPath=/path \
 *  --targetPath=/path \
 *  --esIndexName=test2_java \
 *  --esAlias=occurrence2_java \
 *  --indexNumberShards=3 \
 * --esHosts=http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200 \
 * --esDocumentId=id
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAEventToEsIndexPipeline {

  public static void main(String[] args) throws IOException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "elastic");
    ALAEsIndexingPipelineOptions options = create(ALAEsIndexingPipelineOptions.class, combinedArgs);
    registerHdfs(options);
    run(options);
  }

  public static void run(ALAEsIndexingPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      ALAEsIndexingPipelineOptions options,
      Function<EsIndexingPipelineOptions, Pipeline> pipelinesFn) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    String esDocumentId = options.getEsDocumentId();

    ElasticsearchTools.createIndexAndAliasForDefault(options);

    log.info("Adding step 1: Options");
    UnaryOperator<String> eventsPathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Event, t, ALL_AVRO);

    UnaryOperator<String> occurrencesPathFn =
        t ->
            PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Occurrence, t, ALL_AVRO);

    UnaryOperator<String> identifiersPathFn =
        t -> ALAFsUtils.buildPathIdentifiersUsingTargetPath(options, t, ALL_AVRO);

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    String occurrencesMetadataPath =
        PathBuilder.buildDatasetAttemptPath(options, ValidationUtils.VERBATIM_METRICS, false);

    boolean datasetHasOccurrences = FsUtils.fileExists(hdfsConfigs, occurrencesMetadataPath);
    log.info(
        "Checking for occurrences on path "
            + occurrencesMetadataPath
            + ", has occurrences"
            + datasetHasOccurrences);

    Pipeline p = pipelinesFn.apply(options);

    log.info("Adding step 2: Creating transformations");
    ALAMetadataTransform alaMetadataTransform = ALAMetadataTransform.builder().create();
    // Core
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();
    IdentifierTransform identifierTransform = IdentifierTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    LocationTransform parentLocationTransform = LocationTransform.builder().create();
    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollectionView<ALAMetadataRecord> metadataView =
        p.apply("Read Metadata", alaMetadataTransform.read(eventsPathFn))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Event Verbatim", verbatimTransform.read(eventsPathFn))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, IdentifierRecord>> identifierCollection =
        p.apply("Read Event identifiers", identifierTransform.read(eventsPathFn))
            .apply("Map identifiers to KV", identifierTransform.toKv());

    PCollection<KV<String, EventCoreRecord>> eventCoreCollection =
        p.apply("Read Event core", eventCoreTransform.read(eventsPathFn))
            .apply("Map Event core to KV", eventCoreTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Event Temporal", temporalTransform.read(eventsPathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Event Location", locationTransform.read(eventsPathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    InheritedFields inheritedFields =
        InheritedFields.builder()
            .inheritedFieldsTransform(InheritedFieldsTransform.builder().build())
            .locationCollection(locationCollection)
            .locationTransform(locationTransform)
            .temporalCollection(temporalCollection)
            .temporalTransform(temporalTransform)
            .eventCoreCollection(eventCoreCollection)
            .eventCoreTransform(eventCoreTransform)
            .build();

    PCollection<KV<String, LocationInheritedRecord>> locationInheritedRecords =
        inheritedFields.inheritLocationFields();

    PCollection<KV<String, TemporalInheritedRecord>> temporalInheritedRecords =
        inheritedFields.inheritTemporalFields();

    PCollection<KV<String, EventInheritedRecord>> eventInheritedRecords =
        inheritedFields.inheritEventFields();

    PCollection<KV<String, DerivedMetadataRecord>> derivedMetadataRecordCollection =
        DerivedMetadata.builder()
            .pipeline(p)
            .verbatimTransform(verbatimTransform)
            .temporalTransform(temporalTransform)
            .parentLocationTransform(parentLocationTransform)
            .locationTransform(locationTransform)
            .eventCoreTransform(eventCoreTransform)
            .verbatimCollection(verbatimCollection)
            .temporalCollection(temporalCollection)
            .locationCollection(locationCollection)
            .eventCoreCollection(eventCoreCollection)
            .occurrencesPathFn(occurrencesPathFn)
            .datasetHasOccurrences(datasetHasOccurrences)
            .build()
            .calculate();

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Event Multimedia", multimediaTransform.read(eventsPathFn))
            .apply("Map Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        p.apply("Read Event Image", imageTransform.read(eventsPathFn))
            .apply("Map Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Read Event Audubon", audubonTransform.read(eventsPathFn))
            .apply("Map Audubon to KV", audubonTransform.toKv());

    PCollection<KV<String, MeasurementOrFactRecord>> measurementOrFactCollection =
        p.apply("Read Measurement or fact", measurementOrFactTransform.read(eventsPathFn))
            .apply("Map Measurement or fact to KV", measurementOrFactTransform.toKv());

    log.info("Adding step 3: Converting into a json object");
    SingleOutput<KV<String, CoGbkResult>, String> eventJsonDoFn =
        ALAParentJsonTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .identifierRecordTag(identifierTransform.getTag())
            .eventCoreRecordTag(eventCoreTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .derivedMetadataRecordTag(DerivedMetadataTransform.tag())
            .measurementOrFactRecordTag(measurementOrFactTransform.getTag())
            .locationInheritedRecordTag(InheritedFieldsTransform.LIR_TAG)
            .temporalInheritedRecordTag(InheritedFieldsTransform.TIR_TAG)
            .eventInheritedRecordTag(InheritedFieldsTransform.EIR_TAG)
            .metadataView(metadataView)
            .build()
            .converter();

    PCollection<String> eventJsonCollection =
        KeyedPCollectionTuple
            // Core
            .of(verbatimTransform.getTag(), verbatimCollection)
            .and(identifierTransform.getTag(), identifierCollection)
            .and(eventCoreTransform.getTag(), eventCoreCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            .and(multimediaTransform.getTag(), multimediaCollection)
            .and(imageTransform.getTag(), imageCollection)
            .and(audubonTransform.getTag(), audubonCollection)
            .and(DerivedMetadataTransform.tag(), derivedMetadataRecordCollection)
            .and(measurementOrFactTransform.getTag(), measurementOrFactCollection)
            .and(InheritedFieldsTransform.LIR_TAG, locationInheritedRecords)
            .and(InheritedFieldsTransform.TIR_TAG, temporalInheritedRecords)
            .and(InheritedFieldsTransform.EIR_TAG, eventInheritedRecords)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", eventJsonDoFn);

    PCollection<String> occurrenceJsonCollection =
        datasetHasOccurrences
            ? ALAOccurrenceToEsIndexPipeline.IndexingTransform.builder()
                .pipeline(p)
                .identifiersPathFn(identifiersPathFn)
                .occurrencePathFn(occurrencesPathFn)
                .sensitiveDataCheck(options.getIncludeSensitiveDataChecks())
                .eventsPathFn(eventsPathFn)
                .asParentChildRecord(true)
                .build()
                .apply()
            : p.apply("Create empty occurrenceJsonCollection", Create.empty(StringUtf8Coder.of()));

    // Merge events and occurrences
    PCollection<String> jsonCollection =
        PCollectionList.of(eventJsonCollection)
            .and(occurrenceJsonCollection)
            .apply("Join event and occurrence Json records", Flatten.pCollections());

    log.info("Adding step 6: Elasticsearch indexing");
    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
                options.getEsHosts(), options.getEsIndexName(), "_doc")
            .withConnectTimeout(180000);

    if (Objects.nonNull(options.getEsUsername()) && Objects.nonNull(options.getEsPassword())) {
      esConfig =
          esConfig.withUsername(options.getEsUsername()).withPassword(options.getEsPassword());
    }

    ElasticsearchIO.Write writeIO =
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withRoutingFn(
                new ElasticsearchIO.Write.FieldValueExtractFn() {
                  @Override
                  public String apply(JsonNode input) {
                    return Optional.of(input.get("joinRecord"))
                        .filter(i -> i.hasNonNull("parent"))
                        .map(i -> i.get("parent").asText())
                        .orElse(input.get("internalId").asText());
                  }
                })
            .withMaxBatchSize(options.getEsMaxBatchSize());

    // Ignore gbifID as ES doc ID, useful for validator
    if (esDocumentId != null && !esDocumentId.isEmpty()) {
      writeIO =
          writeIO.withIdFn(
              new ElasticsearchIO.Write.FieldValueExtractFn() {
                @Override
                public String apply(JsonNode input) {
                  return input.get(esDocumentId).asText();
                }
              });
    }

    jsonCollection.apply("Push records to ES", writeIO);

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    log.info("Save metrics into the file and set files owner");
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }

  @Builder
  static class DerivedMetadata {
    private final Pipeline pipeline;
    private final VerbatimTransform verbatimTransform;
    private final TemporalTransform temporalTransform;
    private final LocationTransform parentLocationTransform;
    private final EventCoreTransform eventCoreTransform;
    private final LocationTransform locationTransform;
    private final PCollection<KV<String, ExtendedRecord>> verbatimCollection;
    private final PCollection<KV<String, TemporalRecord>> temporalCollection;
    private final PCollection<KV<String, LocationRecord>> locationCollection;
    private final PCollection<KV<String, EventCoreRecord>> eventCoreCollection;
    private final UnaryOperator<String> occurrencesPathFn;
    private final boolean datasetHasOccurrences;

    /** Calculates the simple Temporal Coverage of an Event. */
    private PCollection<KV<String, EventDate>> temporalCoverage() {
      PCollection<KV<String, TemporalRecord>> eventOccurrenceTemporalCollection =
          datasetHasOccurrences
              ? pipeline
                  .apply(
                      "Read occurrence event temporal records",
                      temporalTransform.read(occurrencesPathFn))
                  .apply(
                      "Remove temporal records with null core ids",
                      Filter.by(NotNullOrEmptyFilter.of(TemporalRecord::getCoreId)))
                  .apply(
                      "Map occurrence events temporal records to KV",
                      temporalTransform.toCoreIdKv())
              : pipeline.apply(
                  "Create empty eventOccurrenceTemporalCollection",
                  Create.empty(new TypeDescriptor<KV<String, TemporalRecord>>() {}));

      // Creates a Map of all events and its sub events
      PCollection<KV<String, TemporalRecord>> temporalRecordsOfSubEvents =
          ParentEventExpandTransform.createTemporalTransform(
                  temporalTransform.getTag(),
                  eventCoreTransform.getTag(),
                  temporalTransform.getEdgeTag())
              .toSubEventsRecords("Temporal", temporalCollection, eventCoreCollection);

      return PCollectionList.of(temporalCollection)
          .and(eventOccurrenceTemporalCollection)
          .and(temporalRecordsOfSubEvents)
          .apply("Joining temporal records", Flatten.pCollections())
          .apply("Calculate the temporal coverage", Combine.perKey(new TemporalCoverageFn()));
    }

    private PCollection<KV<String, String>> convexHull() {
      PCollection<KV<String, LocationRecord>> eventOccurrenceLocationCollection =
          datasetHasOccurrences
              ? pipeline
                  .apply(
                      "Read occurrence events locations",
                      parentLocationTransform.read(occurrencesPathFn))
                  .apply(
                      "Remove location records with null core ids",
                      Filter.by(NotNullOrEmptyFilter.of(LocationRecord::getCoreId)))
                  .apply(
                      "Map occurrence events locations to KV", parentLocationTransform.toCoreIdKv())
              : pipeline.apply(
                  "Create empty eventOccurrenceLocationCollection",
                  Create.empty(new TypeDescriptor<KV<String, LocationRecord>>() {}));

      PCollection<KV<String, LocationRecord>> locationRecordsOfSubEvents =
          ParentEventExpandTransform.createLocationTransform(
                  locationTransform.getTag(),
                  eventCoreTransform.getTag(),
                  locationTransform.getEdgeTag())
              .toSubEventsRecords("Location", locationCollection, eventCoreCollection);

      return PCollectionList.of(locationCollection)
          .and(eventOccurrenceLocationCollection)
          .and(locationRecordsOfSubEvents)
          .apply("Joining location records", Flatten.pCollections())
          .apply(
              "Calculate the WKT Convex Hull of all records", Combine.perKey(new ConvexHullFn()));
    }

    PCollection<KV<String, DerivedMetadataRecord>> calculate() {

      PCollection<KV<String, ExtendedRecord>> eventOccurrenceVerbatimCollection =
          datasetHasOccurrences
              ? pipeline
                  .apply(
                      "Read event occurrences verbatim", verbatimTransform.read(occurrencesPathFn))
                  .apply(
                      "Remove verbatim records with null parent ids",
                      Filter.by(NotNullOrEmptyFilter.of((ExtendedRecord er) -> er.getCoreId())))
                  .apply("Map event occurrences verbatim to KV", verbatimTransform.toParentKv())
              : pipeline.apply(
                  "Create empty eventOccurrenceVerbatimCollection",
                  Create.empty(new TypeDescriptor<KV<String, ExtendedRecord>>() {}));

      return KeyedPCollectionTuple.of(ConvexHullFn.tag(), convexHull())
          .and(TemporalCoverageFn.tag(), temporalCoverage())
          .and(
              verbatimTransform.getTag(),
              PCollectionList.of(eventOccurrenceVerbatimCollection)
                  .and(verbatimCollection)
                  .apply("Join event and occurrence verbatim records", Flatten.pCollections()))
          .apply("Grouping derived metadata data", CoGroupByKey.create())
          .apply(
              "Creating derived metadata records",
              ALADerivedMetadataTransform.builder()
                  .convexHullTag(ConvexHullFn.tag())
                  .temporalCoverageTag(TemporalCoverageFn.tag())
                  .extendedRecordTag(verbatimTransform.getTag())
                  .build()
                  .converter());
    }
  }
}
