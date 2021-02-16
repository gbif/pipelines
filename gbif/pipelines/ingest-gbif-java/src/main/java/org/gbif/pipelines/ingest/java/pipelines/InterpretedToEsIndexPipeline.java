package org.gbif.pipelines.ingest.java.pipelines;

import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.GBIF_ID;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.elasticsearch.action.index.IndexRequest;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.io.ElasticsearchWriter;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.Record;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
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
 *      {@link org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord},
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
    String esDocumentId = options.getEsDocumentId();

    log.info("Creating transformations");
    // Core
    BasicTransform basicTransform = BasicTransform.builder().create();
    MetadataTransform metadataTransform = MetadataTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();
    GrscicollTransform grscicollTransform = GrscicollTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToEsIndexMetrics();

    log.info("Creating pipeline");
    log.info("Reading avro files...");
    // Reading all avro files in parallel
    CompletableFuture<Map<String, MetadataRecord>> metadataMapFeature =
        readAvroAsFuture(options, executor, metadataTransform);

    CompletableFuture<Map<String, ExtendedRecord>> verbatimMapFeature =
        readAvroAsFuture(options, executor, verbatimTransform);

    CompletableFuture<Map<String, BasicRecord>> basicMapFeature =
        readAvroAsFuture(options, executor, basicTransform);

    CompletableFuture<Map<String, TemporalRecord>> temporalMapFeature =
        readAvroAsFuture(options, executor, temporalTransform);

    CompletableFuture<Map<String, LocationRecord>> locationMapFeature =
        readAvroAsFuture(options, executor, locationTransform);

    CompletableFuture<Map<String, TaxonRecord>> taxonMapFeature =
        readAvroAsFuture(options, executor, taxonomyTransform);

    CompletableFuture<Map<String, GrscicollRecord>> grscicollMapFeature =
        readAvroAsFuture(options, executor, grscicollTransform);

    CompletableFuture<Map<String, MultimediaRecord>> multimediaMapFeature =
        readAvroAsFuture(options, executor, multimediaTransform);

    CompletableFuture<Map<String, ImageRecord>> imageMapFeature =
        readAvroAsFuture(options, executor, imageTransform);

    CompletableFuture<Map<String, AudubonRecord>> audubonMapFeature =
        readAvroAsFuture(options, executor, audubonTransform);

    // Get all values
    MetadataRecord metadata = metadataMapFeature.get().values().iterator().next();
    Map<String, BasicRecord> basicMap = basicMapFeature.get();
    Map<String, ExtendedRecord> verbatimMap = verbatimMapFeature.get();
    Map<String, TemporalRecord> temporalMap = temporalMapFeature.get();
    Map<String, LocationRecord> locationMap = locationMapFeature.get();
    Map<String, TaxonRecord> taxonMap = taxonMapFeature.get();
    Map<String, GrscicollRecord> grscicollMap = grscicollMapFeature.get();
    Map<String, MultimediaRecord> multimediaMap = multimediaMapFeature.get();
    Map<String, ImageRecord> imageMap = imageMapFeature.get();
    Map<String, AudubonRecord> audubonMap = audubonMapFeature.get();

    log.info("Joining avro files...");
    // Join all records, convert into string json and IndexRequest for ES
    Function<BasicRecord, IndexRequest> indexRequestFn =
        br -> {
          String k = br.getId();
          // Core
          ExtendedRecord er =
              verbatimMap.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());
          TemporalRecord tr =
              temporalMap.getOrDefault(k, TemporalRecord.newBuilder().setId(k).build());
          LocationRecord lr =
              locationMap.getOrDefault(k, LocationRecord.newBuilder().setId(k).build());
          TaxonRecord txr = taxonMap.getOrDefault(k, TaxonRecord.newBuilder().setId(k).build());
          GrscicollRecord gr =
              grscicollMap.getOrDefault(k, GrscicollRecord.newBuilder().setId(k).build());
          // Extension
          MultimediaRecord mr =
              multimediaMap.getOrDefault(k, MultimediaRecord.newBuilder().setId(k).build());
          ImageRecord ir = imageMap.getOrDefault(k, ImageRecord.newBuilder().setId(k).build());
          AudubonRecord ar =
              audubonMap.getOrDefault(k, AudubonRecord.newBuilder().setId(k).build());

          MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);
          ObjectNode json = GbifJsonConverter.toJson(metadata, br, tr, lr, txr, gr, mmr, er);

          metrics.incMetric(AVRO_TO_JSON_COUNT);

          String docId =
              esDocumentId.equals(GBIF_ID)
                  ? br.getGbifId().toString()
                  : json.get(esDocumentId).asText();

          return new IndexRequest(options.getEsIndexName()).id(docId).source(json.toString(), JSON);
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

  /** Read avro files and return as Map<ID, Clazz> */
  private static <T extends SpecificRecordBase & Record>
      CompletableFuture<Map<String, T>> readAvroAsFuture(
          InterpretationPipelineOptions options,
          ExecutorService executor,
          Transform<?, T> transform) {
    String path =
        PathBuilder.buildPathInterpretUsingInputPath(
            options, transform.getBaseName(), "*" + AVRO_EXTENSION);
    return CompletableFuture.supplyAsync(
        () ->
            AvroReader.readRecords(
                options.getHdfsSiteConfig(),
                options.getCoreSiteConfig(),
                transform.getReturnClazz(),
                path),
        executor);
  }
}
