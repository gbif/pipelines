package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transforms.converters.OccurrenceJsonTransform;
import org.gbif.pipelines.transforms.converters.ParentJsonTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.ClusteringTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
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
public class EventToEsIndexPipeline {

  public static void main(String[] args) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    run(options);
  }

  public static void run(EsIndexingPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      EsIndexingPipelineOptions options,
      Function<EsIndexingPipelineOptions, Pipeline> pipelinesFn) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    String esDocumentId = options.getEsDocumentId();

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Event, t, ALL_AVRO);

    UnaryOperator<String> occurrencesPathFn =
        t ->
            PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Occurrence, t, ALL_AVRO);

    Pipeline p = pipelinesFn.apply(options);

    log.info("Adding step 2: Creating transformations");
    MetadataTransform metadataTransform = MetadataTransform.builder().create();
    // Core
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();
    IdentifierTransform identifierTransform = IdentifierTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();
    BasicTransform basicTransform = BasicTransform.builder().create();
    GbifIdTransform idTransform = GbifIdTransform.builder().create();
    ClusteringTransform clusteringTransform = ClusteringTransform.builder().create();
    GrscicollTransform grscicollTransform = GrscicollTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", metadataTransform.read(pathFn))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", verbatimTransform.read(pathFn))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, IdentifierRecord>> identifierCollection =
        p.apply("Read identifiers", identifierTransform.read(pathFn))
            .apply("Map identifiers to KV", identifierTransform.toKv());

    PCollection<KV<String, EventCoreRecord>> eventCoreCollection =
        p.apply("Read Event core", eventCoreTransform.read(pathFn))
            .apply("Map Event core to KV", eventCoreTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", temporalTransform.read(pathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", locationTransform.read(pathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply("Read Taxon", taxonomyTransform.read(pathFn))
            .apply("Map Taxon to KV", taxonomyTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", multimediaTransform.read(pathFn))
            .apply("Map Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        p.apply("Read Image", imageTransform.read(pathFn))
            .apply("Map Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Read Audubon", audubonTransform.read(pathFn))
            .apply("Map Audubon to KV", audubonTransform.toKv());

    log.info("Adding step 3: Converting into a json object");
    SingleOutput<KV<String, CoGbkResult>, String> eventJsonDoFn =
        ParentJsonTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .identifierRecordTag(identifierTransform.getTag())
            .eventCoreRecordTag(eventCoreTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .taxonRecordTag(taxonomyTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .metadataView(metadataView)
            .build()
            .converter();

    PCollection<String> eventJsonCollection =
        KeyedPCollectionTuple
            // Core
            .of(eventCoreTransform.getTag(), eventCoreCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            .and(taxonomyTransform.getTag(), taxonCollection)
            // Extension
            .and(multimediaTransform.getTag(), multimediaCollection)
            .and(imageTransform.getTag(), imageCollection)
            .and(audubonTransform.getTag(), audubonCollection)
            // Internal
            .and(identifierTransform.getTag(), identifierCollection)
            // Raw
            .and(verbatimTransform.getTag(), verbatimCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", eventJsonDoFn);

    // Occurrnce
    log.info("Adding step 4: Creating occurrence beam pipeline");
    PCollectionView<MetadataRecord> occurrenceMetadataView =
        p.apply("Read occurrence Metadata", metadataTransform.read(occurrencesPathFn))
            .apply("Convert occurrence to view", View.asSingleton());

    PCollection<KV<String, GbifIdRecord>> occurrenceIdCollection =
        p.apply("Read occurrence GBIF ids", idTransform.read(occurrencesPathFn))
            .apply("Map occurrence GBIF ids to KV", idTransform.toKv());

    PCollection<KV<String, ClusteringRecord>> occurrenceClusteringCollection =
        p.apply("Read occurrence clustering", clusteringTransform.read(occurrencesPathFn))
            .apply("Map occurrence clustering to KV", clusteringTransform.toKv());

    PCollection<KV<String, ExtendedRecord>> occurrenceVerbatimCollection =
        p.apply("Read occurrence Verbatim", verbatimTransform.read(occurrencesPathFn))
            .apply("Map occurrence Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, BasicRecord>> occurrenceBasicCollection =
        p.apply("Read occurrence Basic", basicTransform.read(occurrencesPathFn))
            .apply("Map occurrence Basic to KV", basicTransform.toKv());

    PCollection<KV<String, TemporalRecord>> occurrenceTemporalCollection =
        p.apply("Read occurrence Temporal", temporalTransform.read(occurrencesPathFn))
            .apply("Map occurrence Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> occurrenceLocationCollection =
        p.apply("Read occurrence Location", locationTransform.read(occurrencesPathFn))
            .apply("Map occurrence Location to KV", locationTransform.toKv());

    PCollection<KV<String, TaxonRecord>> occurrenceTaxonCollection =
        p.apply("Read occurrence Taxon", taxonomyTransform.read(occurrencesPathFn))
            .apply("Map occurrence Taxon to KV", taxonomyTransform.toKv());

    PCollection<KV<String, GrscicollRecord>> occurrenceGrscicollCollection =
        p.apply("Read occurrence Grscicoll", grscicollTransform.read(occurrencesPathFn))
            .apply("Map occurrence Grscicoll to KV", grscicollTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> occurrenceMultimediaCollection =
        p.apply("Read occurrence Multimedia", multimediaTransform.read(occurrencesPathFn))
            .apply("Map occurrence Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> occurrenceImageCollection =
        p.apply("Read occurrence Image", imageTransform.read(occurrencesPathFn))
            .apply("Map occurrence Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> occurrenceAudubonCollection =
        p.apply("Read occurrence Audubon", audubonTransform.read(occurrencesPathFn))
            .apply("Map occurrence Audubon to KV", audubonTransform.toKv());

    log.info("Adding step 5: Converting into a occurrence json object");
    SingleOutput<KV<String, CoGbkResult>, String> occurrenceJsonDoFn =
        OccurrenceJsonTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .gbifIdRecordTag(idTransform.getTag())
            .clusteringRecordTag(clusteringTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .taxonRecordTag(taxonomyTransform.getTag())
            .grscicollRecordTag(grscicollTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .metadataView(occurrenceMetadataView)
            .asParentChildRecord(true)
            .build()
            .converter();

    PCollection<String> occurrenceJsonCollection =
        KeyedPCollectionTuple
            // Core
            .of(basicTransform.getTag(), occurrenceBasicCollection)
            .and(idTransform.getTag(), occurrenceIdCollection)
            .and(clusteringTransform.getTag(), occurrenceClusteringCollection)
            .and(temporalTransform.getTag(), occurrenceTemporalCollection)
            .and(locationTransform.getTag(), occurrenceLocationCollection)
            .and(taxonomyTransform.getTag(), occurrenceTaxonCollection)
            .and(grscicollTransform.getTag(), occurrenceGrscicollCollection)
            // Extension
            .and(multimediaTransform.getTag(), occurrenceMultimediaCollection)
            .and(imageTransform.getTag(), occurrenceImageCollection)
            .and(audubonTransform.getTag(), occurrenceAudubonCollection)
            // Raw
            .and(verbatimTransform.getTag(), occurrenceVerbatimCollection)
            // Apply
            .apply("Grouping occurrence objects", CoGroupByKey.create())
            .apply("Merging to occurrence json", occurrenceJsonDoFn);

    // Merge events and occurrences
    PCollection<String> jsonCollection =
        PCollectionList.of(eventJsonCollection)
            .and(occurrenceJsonCollection)
            .apply(Flatten.pCollections());

    log.info("Adding step 6: Elasticsearch indexing");
    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getEsHosts(), options.getEsIndexName(), "_doc");

    if (Objects.nonNull(options.getEsUsername()) && Objects.nonNull(options.getEsPassword())) {
      esConfig =
          esConfig.withUsername(options.getEsUsername()).withPassword(options.getEsPassword());
    }

    ElasticsearchIO.Write writeIO =
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withRoutingFn(
                input ->
                    Optional.of(input.get("joinRecord"))
                        .filter(i -> i.hasNonNull("parent"))
                        .map(i -> i.get("parent").asText())
                        .orElse(input.get("internalId").asText()))
            .withMaxBatchSize(options.getEsMaxBatchSize());

    // Ignore gbifID as ES doc ID, useful for validator
    if (esDocumentId != null && !esDocumentId.isEmpty()) {
      writeIO = writeIO.withIdFn(input -> input.get(esDocumentId).asText());
    }

    jsonCollection.apply(writeIO);

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Save metrics into the file and set files owner");
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }
}
