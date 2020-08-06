package org.gbif.pipelines.ingest.java.pipelines;

import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.GBIF_ID;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.INDEX_TYPE;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.IndexRequest;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.ingest.java.io.AvroReader;
import org.gbif.pipelines.ingest.java.io.ElasticsearchWriter;
import org.gbif.pipelines.ingest.java.metrics.IngestMetrics;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaggedValueRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.metadata.TaggedValuesTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads avro files:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *      {@link org.gbif.pipelines.io.avro.BasicRecord},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *      {@link org.gbif.pipelines.io.avro.ImageRecord},
 *      {@link org.gbif.pipelines.io.avro.AudubonRecord},
 *      {@link org.gbif.pipelines.io.avro.MeasurementOrFactRecord},
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord}
 *    2) Joins avro files
 *    3) Converts to json model (resources/elasticsearch/es-occurrence-schema.json)
 *    4) Pushes data to Elasticsearch instance
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline \
 * --datasetId=9f747cff-839f-4485-83a1-f10317a92a82 \
 * --attempt=1 \
 * --inputPath=/path \
 * --targetPath=/path \
 * --esHosts=http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200 \
 * --properties=/path/pipelines.properties \
 * --esIndexName=index_name \
 * --esAlias=index_alias \
 * --indexNumberShards=1 \
 * --esDocumentId=id
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedToEsIndexPipeline {

  public static void main(String[] args) {
    run(args);
  }

  public static void run(String[] args) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    run(options);
  }

  public static void run(EsIndexingPipelineOptions options) {
    ExecutorService executor = Executors.newWorkStealingPool();
    try {
      run(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  public static void run(String[] args, ExecutorService executor) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    run(options, executor);
  }

  @SneakyThrows
  public static void run(EsIndexingPipelineOptions options, ExecutorService executor) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    log.info("Options");
    UnaryOperator<String> pathFn =
        t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    String esDocumentId = options.getEsDocumentId();
    String hdfsSiteConfig = options.getHdfsSiteConfig();
    String coreSiteConfig = options.getCoreSiteConfig();

    log.info("Creating transformations");
    // Core
    BasicTransform basicTransform = BasicTransform.builder().create();
    MetadataTransform metadataTransform = MetadataTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    TaggedValuesTransform taggedValuesTransform = TaggedValuesTransform.builder().create();

    // Extension
    MeasurementOrFactTransform measurementTransform = MeasurementOrFactTransform.create();
    MultimediaTransform multimediaTransform = MultimediaTransform.create();
    AudubonTransform audubonTransform = AudubonTransform.create();
    ImageTransform imageTransform = ImageTransform.create();

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToEsIndexMetrics();

    log.info("Creating pipeline");
    log.info("Reading avro files...");
    // Reading all avro files in parallel
    CompletableFuture<Map<String, MetadataRecord>> metadataMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    MetadataRecord.class,
                    pathFn.apply(metadataTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, ExtendedRecord>> verbatimMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    ExtendedRecord.class,
                    pathFn.apply(verbatimTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, TaggedValueRecord>> taggedValuesMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    TaggedValueRecord.class,
                    pathFn.apply(taggedValuesTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, BasicRecord>> basicMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    BasicRecord.class,
                    pathFn.apply(basicTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, TemporalRecord>> temporalMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    TemporalRecord.class,
                    pathFn.apply(temporalTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, LocationRecord>> locationMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    LocationRecord.class,
                    pathFn.apply(locationTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, TaxonRecord>> taxonMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    TaxonRecord.class,
                    pathFn.apply(taxonomyTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, MultimediaRecord>> multimediaMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    MultimediaRecord.class,
                    pathFn.apply(multimediaTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, ImageRecord>> imageMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    ImageRecord.class,
                    pathFn.apply(imageTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, AudubonRecord>> audubonMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    AudubonRecord.class,
                    pathFn.apply(audubonTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, MeasurementOrFactRecord>> measurementMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    MeasurementOrFactRecord.class,
                    pathFn.apply(measurementTransform.getBaseName())),
            executor);

    CompletableFuture.allOf(
        metadataMapFeature,
        verbatimMapFeature,
        basicMapFeature,
        temporalMapFeature,
        locationMapFeature,
        taxonMapFeature,
        multimediaMapFeature,
        imageMapFeature,
        audubonMapFeature,
        measurementMapFeature);

    MetadataRecord metadata = metadataMapFeature.get().values().iterator().next();
    Map<String, BasicRecord> basicMap = basicMapFeature.get();
    Map<String, ExtendedRecord> verbatimMap = verbatimMapFeature.get();
    Map<String, TaggedValueRecord> taggedValueRecordMap = taggedValuesMapFeature.get();
    Map<String, TemporalRecord> temporalMap = temporalMapFeature.get();
    Map<String, LocationRecord> locationMap = locationMapFeature.get();
    Map<String, TaxonRecord> taxonMap = taxonMapFeature.get();
    Map<String, MultimediaRecord> multimediaMap = multimediaMapFeature.get();
    Map<String, ImageRecord> imageMap = imageMapFeature.get();
    Map<String, AudubonRecord> audubonMap = audubonMapFeature.get();
    Map<String, MeasurementOrFactRecord> measurementMap = measurementMapFeature.get();

    log.info("Joining avro files...");
    // Join all records, convert into string json and IndexRequest for ES
    Function<BasicRecord, IndexRequest> indexRequestFn =
        br -> {
          String k = br.getId();
          // Core
          ExtendedRecord er =
              verbatimMap.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());
          TaggedValueRecord tvr =
              taggedValueRecordMap.getOrDefault(k, TaggedValueRecord.newBuilder().setId(k).build());
          TemporalRecord tr =
              temporalMap.getOrDefault(k, TemporalRecord.newBuilder().setId(k).build());
          LocationRecord lr =
              locationMap.getOrDefault(k, LocationRecord.newBuilder().setId(k).build());
          TaxonRecord txr = taxonMap.getOrDefault(k, TaxonRecord.newBuilder().setId(k).build());
          // Extension
          MultimediaRecord mr =
              multimediaMap.getOrDefault(k, MultimediaRecord.newBuilder().setId(k).build());
          ImageRecord ir = imageMap.getOrDefault(k, ImageRecord.newBuilder().setId(k).build());
          AudubonRecord ar =
              audubonMap.getOrDefault(k, AudubonRecord.newBuilder().setId(k).build());
          MeasurementOrFactRecord mfr =
              measurementMap.getOrDefault(k, MeasurementOrFactRecord.newBuilder().setId(k).build());

          MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);
          ObjectNode json = GbifJsonConverter.toJson(metadata, br, tr, lr, txr, mmr, mfr, tvr, er);

          metrics.incMetric(AVRO_TO_JSON_COUNT);

          String docId =
              esDocumentId.equals(GBIF_ID)
                  ? br.getGbifId().toString()
                  : json.get(esDocumentId).asText();

          return new IndexRequest(options.getEsIndexName(), INDEX_TYPE, docId)
              .source(json.toString(), JSON);
        };

    boolean useSyncMode = options.getSyncThreshold() > basicMap.size();

    log.info("Pushing data into Elasticsearch");
    ElasticsearchWriter.<BasicRecord>builder()
        .esHosts(options.getEsHosts())
        .esMaxBatchSize(options.getEsMaxBatchSize())
        .esMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
        .executor(executor)
        .useSyncMode(useSyncMode)
        .indexRequestFn(indexRequestFn)
        .records(basicMap.values())
        .build()
        .write();

    MetricsHandler.saveCountersToTargetPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }
}
