package au.org.ala.pipelines.java;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.common.ALARecordTypes;
import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.pipelines.transforms.ALAAttributionTransform;
import au.org.ala.pipelines.transforms.ALASolrDocumentTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.util.Collections;
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
import org.apache.solr.common.SolrInputDocument;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.ingest.java.io.AvroReader;
import org.gbif.pipelines.ingest.java.metrics.IngestMetrics;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.*;
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
import org.gbif.pipelines.transforms.specific.LocationFeatureTransform;
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
public class ALAInterpretedToSolrIndexPipeline {

  public static void main(String[] args) throws FileNotFoundException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "index");
    run(combinedArgs);
  }

  public static void run(String[] args) {
    ALASolrPipelineOptions options =
        PipelinesOptionsFactory.create(ALASolrPipelineOptions.class, args);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(ALASolrPipelineOptions options) {
    ExecutorService executor = Executors.newWorkStealingPool();
    try {
      run(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  @SneakyThrows
  public static void run(ALASolrPipelineOptions options, ExecutorService executor) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    log.info("Options");

    UnaryOperator<String> pathFn =
        t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);
    UnaryOperator<String> identifiersPathFn =
        t -> ALAFsUtils.buildPathIdentifiersUsingTargetPath(options, t, "*" + AVRO_EXTENSION);
    UnaryOperator<String> samplingPathFn =
        t -> ALAFsUtils.buildPathSamplingUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    String hdfsSiteConfig = options.getHdfsSiteConfig();
    String coreSiteConfig = options.getCoreSiteConfig();

    log.info("Creating transformations");
    // Core
    BasicTransform basicTransform = BasicTransform.builder().create();
    MetadataTransform metadataTransform = MetadataTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();
    //        TaggedValuesTransform taggedValuesTransform = TaggedValuesTransform.create();

    // Extension
    MeasurementOrFactTransform measurementTransform = MeasurementOrFactTransform.create();
    MultimediaTransform multimediaTransform = MultimediaTransform.create();
    AudubonTransform audubonTransform = AudubonTransform.create();
    ImageTransform imageTransform = ImageTransform.create();

    // ALA Specific transforms
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();
    ALAAttributionTransform alaAttributionTransform = ALAAttributionTransform.builder().create();
    LocationFeatureTransform spatialTransform = LocationFeatureTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToEsIndexMetrics();

    log.info("Creating pipeline");
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

    //        CompletableFuture<Map<String, TaggedValueRecord>> taggedValuesMapFeature =
    // CompletableFuture.supplyAsync(
    //                () -> AvroReader.readRecords(hdfsSiteConfig, TaggedValueRecord.class,
    // pathFn.apply(taggedValuesTransform.getBaseName())),
    //                executor);

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

    CompletableFuture<Map<String, TaxonRecord>> taxonMapFeature = null;
    if (options.getIncludeGbifTaxonomy()) {
      taxonMapFeature =
          CompletableFuture.supplyAsync(
              () ->
                  AvroReader.readRecords(
                      hdfsSiteConfig,
                      coreSiteConfig,
                      TaxonRecord.class,
                      pathFn.apply(taxonomyTransform.getBaseName())),
              executor);
    }

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
        multimediaMapFeature,
        imageMapFeature,
        audubonMapFeature,
        measurementMapFeature);

    // ALA Specific
    CompletableFuture<Map<String, ALAUUIDRecord>> alaUuidMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    ALAUUIDRecord.class,
                    identifiersPathFn.apply(ALARecordTypes.ALA_UUID.name().toLowerCase())),
            executor);

    CompletableFuture<Map<String, ALATaxonRecord>> alaTaxonMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    ALATaxonRecord.class,
                    pathFn.apply(alaTaxonomyTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, ALAAttributionRecord>> alaAttributionMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    ALAAttributionRecord.class,
                    pathFn.apply(alaAttributionTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, LocationFeatureRecord>> australiaSpatialMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsSiteConfig,
                    coreSiteConfig,
                    LocationFeatureRecord.class,
                    samplingPathFn.apply(spatialTransform.getBaseName())),
            executor);

    MetadataRecord metadata = metadataMapFeature.get().values().iterator().next();
    Map<String, BasicRecord> basicMap = basicMapFeature.get();
    Map<String, ExtendedRecord> verbatimMap = verbatimMapFeature.get();
    Map<String, TemporalRecord> temporalMap = temporalMapFeature.get();
    Map<String, LocationRecord> locationMap = locationMapFeature.get();

    Map<String, ALAUUIDRecord> aurMap = alaUuidMapFeature.get();
    Map<String, ALATaxonRecord> alaTaxonMap = alaTaxonMapFeature.get();
    Map<String, ALAAttributionRecord> alaAttributionMap = alaAttributionMapFeature.get();
    Map<String, LocationFeatureRecord> australiaSpatialMap =
        options.getIncludeSampling() ? australiaSpatialMapFeature.get() : Collections.emptyMap();

    Map<String, MultimediaRecord> multimediaMap = multimediaMapFeature.get();
    Map<String, ImageRecord> imageMap = imageMapFeature.get();
    Map<String, AudubonRecord> audubonMap = audubonMapFeature.get();
    Map<String, MeasurementOrFactRecord> measurementMap = measurementMapFeature.get();

    log.info("Joining avro files...");
    // Join all records, convert into string json and IndexRequest for ES
    Function<BasicRecord, SolrInputDocument> indexRequestFn =
        br -> {
          String k = br.getId();

          // Core
          ExtendedRecord er =
              verbatimMap.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());
          TemporalRecord tr =
              temporalMap.getOrDefault(k, TemporalRecord.newBuilder().setId(k).build());
          LocationRecord lr =
              locationMap.getOrDefault(k, LocationRecord.newBuilder().setId(k).build());
          TaxonRecord txr = null;

          //            if (options.getIncludeGbifTaxonomy()) {
          //                txr = taxonMap.getOrDefault(k,
          // TaxonRecord.newBuilder().setId(k).build());
          //            }

          // ALA specific
          ALAUUIDRecord aur = aurMap.getOrDefault(k, ALAUUIDRecord.newBuilder().setId(k).build());
          ALATaxonRecord atxr =
              alaTaxonMap.getOrDefault(k, ALATaxonRecord.newBuilder().setId(k).build());
          ALAAttributionRecord aar =
              alaAttributionMap.getOrDefault(k, ALAAttributionRecord.newBuilder().setId(k).build());
          LocationFeatureRecord asr =
              australiaSpatialMap.getOrDefault(
                  k, LocationFeatureRecord.newBuilder().setId(k).build());

          // Extension
          MultimediaRecord mr =
              multimediaMap.getOrDefault(k, MultimediaRecord.newBuilder().setId(k).build());
          ImageRecord ir = imageMap.getOrDefault(k, ImageRecord.newBuilder().setId(k).build());
          AudubonRecord ar =
              audubonMap.getOrDefault(k, AudubonRecord.newBuilder().setId(k).build());
          MeasurementOrFactRecord mfr =
              measurementMap.getOrDefault(k, MeasurementOrFactRecord.newBuilder().setId(k).build());

          MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);

          return ALASolrDocumentTransform.createSolrDocument(
              metadata, br, tr, lr, txr, atxr, er, aar, asr, aur);
        };

    boolean useSyncMode = options.getSyncThreshold() > basicMap.size();

    log.info("Pushing data into SOLR");
    SolrWriter.<BasicRecord>builder()
        .executor(executor)
        .zkHost(options.getZkHost())
        .collection(options.getSolrCollection())
        .solrMaxBatchSize(options.getSolrBatchSize())
        .useSyncMode(useSyncMode)
        .indexRequestFn(indexRequestFn)
        .records(basicMap.values())
        .build()
        .write();

    MetricsHandler.saveCountersToTargetPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }
}
