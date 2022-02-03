package org.gbif.pipelines.ingest.java.pipelines;

import static org.gbif.pipelines.ingest.java.transforms.InterpretedAvroReader.readAvroAsFuture;

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
import org.elasticsearch.action.index.IndexRequest;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.ElasticsearchWriter;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.ingest.java.transforms.IndexRequestConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
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

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToEsIndexMetrics();

    log.info("Creating pipeline");
    log.info("Reading avro files...");
    // Reading all avro files in parallel
    CompletableFuture<Map<String, MetadataRecord>> metadataMapFeature =
        readAvroAsFuture(options, executor, MetadataTransform.builder().create());

    CompletableFuture<Map<String, ExtendedRecord>> verbatimMapFeature =
        readAvroAsFuture(options, executor, VerbatimTransform.create());

    CompletableFuture<Map<String, BasicRecord>> basicMapFeature =
        readAvroAsFuture(options, executor, BasicTransform.builder().create());

    CompletableFuture<Map<String, TemporalRecord>> temporalMapFeature =
        readAvroAsFuture(options, executor, TemporalTransform.builder().create());

    CompletableFuture<Map<String, LocationRecord>> locationMapFeature =
        readAvroAsFuture(options, executor, LocationTransform.builder().create());

    CompletableFuture<Map<String, TaxonRecord>> taxonMapFeature =
        readAvroAsFuture(options, executor, TaxonomyTransform.builder().create());

    CompletableFuture<Map<String, GrscicollRecord>> grscicollMapFeature =
        readAvroAsFuture(options, executor, GrscicollTransform.builder().create());

    CompletableFuture<Map<String, MultimediaRecord>> multimediaMapFeature =
        readAvroAsFuture(options, executor, MultimediaTransform.builder().create());

    CompletableFuture<Map<String, ImageRecord>> imageMapFeature =
        readAvroAsFuture(options, executor, ImageTransform.builder().create());

    CompletableFuture<Map<String, AudubonRecord>> audubonMapFeature =
        readAvroAsFuture(options, executor, AudubonTransform.builder().create());

    Function<BasicRecord, IndexRequest> indexRequestFn =
        IndexRequestConverter.builder()
            .metrics(metrics)
            .esIndexName(options.getEsIndexName())
            .esDocumentId(options.getEsDocumentId())
            .metadata(metadataMapFeature.get().values().iterator().next())
            .verbatimMap(verbatimMapFeature.get())
            .temporalMap(temporalMapFeature.get())
            .locationMap(locationMapFeature.get())
            .taxonMap(taxonMapFeature.get())
            .grscicollMap(grscicollMapFeature.get())
            .multimediaMap(multimediaMapFeature.get())
            .imageMap(imageMapFeature.get())
            .audubonMap(audubonMapFeature.get())
            .build()
            .getFn();

    log.info("Pushing data into Elasticsearch");
    ElasticsearchWriter.<BasicRecord>builder()
        .esHosts(options.getEsHosts())
        .esMaxBatchSize(options.getEsMaxBatchSize())
        .esMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
        .executor(executor)
        .syncModeThreshold(options.getSyncThreshold())
        .indexRequestFn(indexRequestFn)
        .records(basicMapFeature.get().values())
        .backPressure(options.getBackPressure())
        .build()
        .write();

    MetricsHandler.saveCountersToTargetPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }
}
