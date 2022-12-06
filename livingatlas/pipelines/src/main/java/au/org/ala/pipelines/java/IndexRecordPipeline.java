package au.org.ala.pipelines.java;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.pipelines.beam.ALAOccurrenceToSearchAvroPipeline;
import au.org.ala.pipelines.common.ALARecordTypes;
import au.org.ala.pipelines.options.IndexingPipelineOptions;
import au.org.ala.pipelines.transforms.ALAAttributionTransform;
import au.org.ala.pipelines.transforms.ALASensitiveDataRecordTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.transforms.IndexRecordTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationResult;
import au.org.ala.utils.ValidationUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
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
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord}
 *    2) Joins avro files
 *    3) Converts to IndexRecord
 * </pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IndexRecordPipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws IOException {
    VersionInfo.print();
    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "speciesLists", "index");
    run(combinedArgs);
    System.exit(0);
  }

  public static void run(String[] args) {
    IndexingPipelineOptions options =
        PipelinesOptionsFactory.create(IndexingPipelineOptions.class, args);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(IndexingPipelineOptions options) {
    ExecutorService executor = Executors.newWorkStealingPool();
    try {
      run(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  @SneakyThrows
  public static void run(IndexingPipelineOptions options, ExecutorService executor) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    options.setMetaFileName(ValidationUtils.INDEXING_METRICS);

    ValidationResult validResult = ValidationUtils.checkReadyForIndexing(options);
    if (!validResult.getValid()) {
      log.error(
          "The dataset can not be indexed. See logs for more details: {}",
          validResult.getMessage());
      return;
    }

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    // get filesystem
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, options.getInputPath());

    final long lastLoadedDate =
        ValidationUtils.metricsModificationTime(
            fs,
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt(),
            ValidationUtils.VERBATIM_METRICS);
    final long lastProcessedDate =
        ValidationUtils.metricsModificationTime(
            fs,
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt(),
            ValidationUtils.INTERPRETATION_METRICS);

    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, ALL_AVRO);
    UnaryOperator<String> identifiersPathFn =
        t -> ALAFsUtils.buildPathIdentifiersUsingTargetPath(options, t, ALL_AVRO);
    UnaryOperator<String> imageServicePathFn =
        t -> ALAFsUtils.buildPathImageServiceUsingTargetPath(options, t, ALL_AVRO);

    log.info("Creating transformations");
    // Core
    BasicTransform basicTransform = BasicTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();

    // ALA Specific transforms
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();
    ALAAttributionTransform alaAttributionTransform = ALAAttributionTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    ALASensitiveDataRecordTransform sensitiveTransform =
        ALASensitiveDataRecordTransform.builder().create();

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToEsIndexMetrics();

    log.info("Creating pipeline");

    // Reading all avro files in parallel
    CompletableFuture<Map<String, ExtendedRecord>> verbatimMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsConfigs,
                    ExtendedRecord.class,
                    pathFn.apply(verbatimTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, BasicRecord>> basicMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsConfigs, BasicRecord.class, pathFn.apply(basicTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, TemporalRecord>> temporalMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsConfigs,
                    TemporalRecord.class,
                    pathFn.apply(temporalTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, LocationRecord>> locationMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsConfigs,
                    LocationRecord.class,
                    pathFn.apply(locationTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, MultimediaRecord>> multimediaFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsConfigs,
                    MultimediaRecord.class,
                    pathFn.apply(multimediaTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, TaxonRecord>> taxonMapFeature =
        CompletableFuture.completedFuture(Collections.emptyMap());
    if (options.getIncludeGbifTaxonomy()) {
      taxonMapFeature =
          CompletableFuture.supplyAsync(
              () ->
                  AvroReader.readRecords(
                      hdfsConfigs,
                      TaxonRecord.class,
                      pathFn.apply(taxonomyTransform.getBaseName())),
              executor);
    }

    CompletableFuture.allOf(
            verbatimMapFeature,
            basicMapFeature,
            temporalMapFeature,
            locationMapFeature,
            taxonMapFeature)
        .get();

    // ALA Specific
    CompletableFuture<Map<String, ALAUUIDRecord>> alaUuidMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsConfigs,
                    ALAUUIDRecord.class,
                    identifiersPathFn.apply(ALARecordTypes.ALA_UUID.name().toLowerCase())),
            executor);

    CompletableFuture<Map<String, ALATaxonRecord>> alaTaxonMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsConfigs,
                    ALATaxonRecord.class,
                    pathFn.apply(alaTaxonomyTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, ALAAttributionRecord>> alaAttributionMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsConfigs,
                    ALAAttributionRecord.class,
                    pathFn.apply(alaAttributionTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, ALASensitivityRecord>> alaSensitiveMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsConfigs,
                    ALASensitivityRecord.class,
                    pathFn.apply(sensitiveTransform.getBaseName())),
            executor);

    CompletableFuture<Map<String, ImageRecord>> imageServiceMapFeature =
        CompletableFuture.supplyAsync(
            () ->
                AvroReader.readRecords(
                    hdfsConfigs, ImageRecord.class, imageServicePathFn.apply("image-record")),
            executor);

    CompletableFuture<Map<String, TaxonProfile>> taxonProfileMapFeature = null;
    if (options.getIncludeSpeciesLists()) {
      taxonProfileMapFeature =
          CompletableFuture.supplyAsync(
              () -> SpeciesListPipeline.generateTaxonProfileCollection(options), executor);
    }

    Map<String, BasicRecord> basicMap = basicMapFeature.get();
    Map<String, ExtendedRecord> verbatimMap = verbatimMapFeature.get();
    Map<String, TemporalRecord> temporalMap = temporalMapFeature.get();
    Map<String, LocationRecord> locationMap = locationMapFeature.get();
    Map<String, ALAUUIDRecord> aurMap = alaUuidMapFeature.get();

    Map<String, MultimediaRecord> mrMap = multimediaFeature.get();
    Map<String, ALATaxonRecord> alaTaxonMap = alaTaxonMapFeature.get();
    Map<String, ALAAttributionRecord> alaAttributionMap = alaAttributionMapFeature.get();
    Map<String, ALASensitivityRecord> alaSensitivityMap =
        options.getIncludeSensitiveDataChecks()
            ? alaSensitiveMapFeature.get()
            : Collections.emptyMap();
    Map<String, ImageRecord> imageServiceMap =
        options.getIncludeImages() ? imageServiceMapFeature.get() : Collections.emptyMap();

    Map<String, TaxonProfile> taxonProfileMap =
        options.getIncludeSpeciesLists() ? taxonProfileMapFeature.get() : Collections.emptyMap();

    log.info("Joining avro files...");
    // Join all records, convert into string json and IndexRequest for ES
    Function<BasicRecord, IndexRecord> indexRequestFn =
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

          // ALA specific
          ALAUUIDRecord aur = aurMap.getOrDefault(k, null);
          MultimediaRecord mr = mrMap.getOrDefault(k, null);
          ALATaxonRecord atxr =
              alaTaxonMap.getOrDefault(k, ALATaxonRecord.newBuilder().setId(k).build());
          ALAAttributionRecord aar =
              alaAttributionMap.getOrDefault(k, ALAAttributionRecord.newBuilder().setId(k).build());
          ALASensitivityRecord sr = alaSensitivityMap.getOrDefault(k, null);
          ImageRecord isr =
              imageServiceMap.getOrDefault(k, ImageRecord.newBuilder().setId(k).build());
          TaxonProfile tpr =
              taxonProfileMap.getOrDefault(k, TaxonProfile.newBuilder().setId(k).build());

          return IndexRecordTransform.createIndexRecord(
              br,
              tr,
              lr,
              txr,
              atxr,
              er,
              aar,
              aur,
              isr,
              tpr,
              sr,
              mr,
              lastLoadedDate,
              lastProcessedDate);
        };

    List<IndexRecord> indexRecords =
        basicMap.values().stream().map(indexRequestFn).collect(Collectors.toList());

    OutputStream output =
        fs.create(
            new Path(
                options.getAllDatasetsInputPath()
                    + "/index-record/"
                    + options.getDatasetId()
                    + "/"
                    + options.getDatasetId()
                    + ".avro"));

    DatumWriter<IndexRecord> datumWriter = new GenericDatumWriter<>(IndexRecord.getClassSchema());
    try (DataFileWriter<IndexRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.setCodec(BASE_CODEC);
      dataFileWriter.create(IndexRecord.getClassSchema(), output);

      for (IndexRecord indexRecord : indexRecords) {
        dataFileWriter.append(indexRecord);
      }
    }

    MetricsHandler.saveCountersToTargetPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());

    // run occurrence AVRO pipeline
    ALAOccurrenceToSearchAvroPipeline.run(options);
  }
}
