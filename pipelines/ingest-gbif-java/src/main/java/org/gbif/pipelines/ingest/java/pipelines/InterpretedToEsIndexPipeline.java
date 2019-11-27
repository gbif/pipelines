package org.gbif.pipelines.ingest.java.pipelines;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.ingest.java.metrics.IngestMetrics;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.ingest.java.readers.AvroRecordReader;
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
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.MetadataTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.MDC;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.GBIF_ID;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.INDEX_TYPE;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedToEsIndexPipeline {

  public static void main(String[] args) {
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

  @SneakyThrows
  public static void run(EsIndexingPipelineOptions options, ExecutorService executor) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    log.info("Options");
    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    String esDocumentId = options.getEsDocumentId();

    log.info("Creating transformations");
    // Core
    BasicTransform basicTransform = BasicTransform.create();
    MetadataTransform metadataTransform = MetadataTransform.create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.create();
    LocationTransform locationTransform = LocationTransform.create();
    // Extension
    MeasurementOrFactTransform measurementTransform = MeasurementOrFactTransform.create();
    MultimediaTransform multimediaTransform = MultimediaTransform.create();
    AudubonTransform audubonTransform = AudubonTransform.create();
    ImageTransform imageTransform = ImageTransform.create();

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToEsIndexMetrics();

    log.info("Creating pipeline");

    // Reading all avro files in parallel
    CompletableFuture<Map<String, MetadataRecord>> metadataMapFeature = CompletableFuture.supplyAsync(
        () -> AvroRecordReader.readRecords(MetadataRecord.class, pathFn.apply(metadataTransform.getBaseName())),
        executor);

    CompletableFuture<Map<String, ExtendedRecord>> verbatimMapFeature = CompletableFuture.supplyAsync(
        () -> AvroRecordReader.readRecords(ExtendedRecord.class, pathFn.apply(verbatimTransform.getBaseName())),
        executor);

    CompletableFuture<Map<String, BasicRecord>> basicMapFeature = CompletableFuture.supplyAsync(
        () -> AvroRecordReader.readRecords(BasicRecord.class, pathFn.apply(basicTransform.getBaseName())),
        executor);

    CompletableFuture<Map<String, TemporalRecord>> temporalMapFeature = CompletableFuture.supplyAsync(
        () -> AvroRecordReader.readRecords(TemporalRecord.class, pathFn.apply(temporalTransform.getBaseName())),
        executor);

    CompletableFuture<Map<String, LocationRecord>> locationMapFeature = CompletableFuture.supplyAsync(
        () -> AvroRecordReader.readRecords(LocationRecord.class, pathFn.apply(locationTransform.getBaseName())),
        executor);

    CompletableFuture<Map<String, TaxonRecord>> taxonMapFeature = CompletableFuture.supplyAsync(
        () -> AvroRecordReader.readRecords(TaxonRecord.class, pathFn.apply(taxonomyTransform.getBaseName())),
        executor);

    CompletableFuture<Map<String, MultimediaRecord>> multimediaMapFeature = CompletableFuture.supplyAsync(
        () -> AvroRecordReader.readRecords(MultimediaRecord.class, pathFn.apply(multimediaTransform.getBaseName())),
        executor);

    CompletableFuture<Map<String, ImageRecord>> imageMapFeature = CompletableFuture.supplyAsync(
        () -> AvroRecordReader.readRecords(ImageRecord.class, pathFn.apply(imageTransform.getBaseName())),
        executor);

    CompletableFuture<Map<String, AudubonRecord>> audubonMapFeature = CompletableFuture.supplyAsync(
        () -> AvroRecordReader.readRecords(AudubonRecord.class, pathFn.apply(audubonTransform.getBaseName())),
        executor);

    CompletableFuture<Map<String, MeasurementOrFactRecord>> measurementMapFeature = CompletableFuture.supplyAsync(
        () -> AvroRecordReader.readRecords(MeasurementOrFactRecord.class, pathFn.apply(measurementTransform.getBaseName())),
        executor);

    CompletableFuture.allOf(metadataMapFeature, verbatimMapFeature, basicMapFeature, temporalMapFeature,
        locationMapFeature, taxonMapFeature, multimediaMapFeature, imageMapFeature, audubonMapFeature, measurementMapFeature);

    MetadataRecord metadata = metadataMapFeature.get().values().iterator().next();
    Map<String, BasicRecord> basicMap = basicMapFeature.get();
    Map<String, ExtendedRecord> verbatimMap = verbatimMapFeature.get();
    Map<String, TemporalRecord> temporalMap = temporalMapFeature.get();
    Map<String, LocationRecord> locationMap = locationMapFeature.get();
    Map<String, TaxonRecord> taxonMap = taxonMapFeature.get();
    Map<String, MultimediaRecord> multimediaMap = multimediaMapFeature.get();
    Map<String, ImageRecord> imageMap = imageMapFeature.get();
    Map<String, AudubonRecord> audubonMap = audubonMapFeature.get();
    Map<String, MeasurementOrFactRecord> measurementMap = measurementMapFeature.get();

    // Join all records, convert into string json and IndexRequest for ES
    Function<BasicRecord, IndexRequest> indexRequestFn = br -> {

      String k = br.getId();
      // Core
      ExtendedRecord er = verbatimMap.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());
      TemporalRecord tr = temporalMap.getOrDefault(k, TemporalRecord.newBuilder().setId(k).build());
      LocationRecord lr = locationMap.getOrDefault(k, LocationRecord.newBuilder().setId(k).build());
      TaxonRecord txr = taxonMap.getOrDefault(k, TaxonRecord.newBuilder().setId(k).build());
      // Extension
      MultimediaRecord mr = multimediaMap.getOrDefault(k, MultimediaRecord.newBuilder().setId(k).build());
      ImageRecord ir = imageMap.getOrDefault(k, ImageRecord.newBuilder().setId(k).build());
      AudubonRecord ar = audubonMap.getOrDefault(k, AudubonRecord.newBuilder().setId(k).build());
      MeasurementOrFactRecord mfr = measurementMap.getOrDefault(k, MeasurementOrFactRecord.newBuilder().setId(k).build());

      MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);
      ObjectNode json = GbifJsonConverter.toJson(metadata, br, tr, lr, txr, mmr, mfr, er);

      metrics.incMetric(AVRO_TO_JSON_COUNT);

      String docId = esDocumentId.equals(GBIF_ID) ? br.getGbifId().toString() : json.get(esDocumentId).asText();

      return new IndexRequest(options.getEsIndexName(), INDEX_TYPE, docId).source(json.toString(), JSON);
    };

    // Create ES client and extra function
    HttpHost[] hosts = Arrays.stream(options.getEsHosts()).map(HttpHost::create).toArray(HttpHost[]::new);
    try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(hosts))) {

      Queue<BulkRequest> requests = new LinkedList<>();
      requests.add(new BulkRequest().timeout(TimeValue.timeValueMinutes(5L)));

      Consumer<BasicRecord> addIndexRequestFn = br -> Optional.ofNullable(requests.peek())
          .ifPresent(req -> req.add(indexRequestFn.apply(br)));

      Runnable pushIntoEsFn = () -> Optional.ofNullable(requests.poll())
          .ifPresent(br -> {
            try {
              client.bulk(br, RequestOptions.DEFAULT);
            } catch (IOException ex) {
              log.error(ex.getMessage(), ex);
            }
          });

      // Push requests into ES
      long counter = 0;
      for (BasicRecord br : basicMap.values()) {
        if (counter < options.getEsMaxBatchSize()) {
          addIndexRequestFn.accept(br);
          counter++;
        } else {
          addIndexRequestFn.accept(br);
          pushIntoEsFn.run();
          requests.add(new BulkRequest().timeout(TimeValue.timeValueMinutes(5L)));
          counter = 0;
        }
      }
      pushIntoEsFn.run();
    }

    MetricsHandler.saveCountersToTargetPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());

  }
}
