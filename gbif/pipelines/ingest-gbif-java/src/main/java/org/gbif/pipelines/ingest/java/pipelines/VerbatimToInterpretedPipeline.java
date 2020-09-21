package org.gbif.pipelines.ingest.java.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.ALL;
import static org.gbif.pipelines.core.utils.FsUtils.createParentDirectories;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.factory.ConfigFactory;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.core.io.SyncDataFileWriterBuilder;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.factory.*;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.java.DefaultValuesTransform;
import org.gbif.pipelines.transforms.java.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.java.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file to:
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
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.VerbatimToInterpretedPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.VerbatimToInterpretedPipeline \
 * --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 * --attempt=1 \
 * --interpretationTypes=ALL \
 * --targetPath=/path \
 * --inputPath=/path/verbatim.avro \
 * --properties=/path/pipelines.properties \
 * --useExtendedRecordId=true
 * --skipRegisrtyCalls=true
 *
 * }</pre>
 */
@SuppressWarnings("all")
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimToInterpretedPipeline {

  public static void main(String[] args) {
    run(args);
  }

  public static void run(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    ExecutorService executor = Executors.newWorkStealingPool();
    try {
      run(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  public static void run(String[] args, ExecutorService executor) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options, executor);
  }

  public static void run(InterpretationPipelineOptions options, ExecutorService executor) {

    log.info("Pipeline has been started - {}", LocalDateTime.now());

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    boolean tripletValid = options.isTripletValid();
    boolean occIdValid = options.isOccurrenceIdValid();
    boolean useErdId = options.isUseExtendedRecordId();
    Set<String> types = Collections.singleton(ALL.name());
    String targetPath = options.getTargetPath();
    String endPointType = options.getEndPointType();
    String hdfsSiteConfig = options.getHdfsSiteConfig();
    String coreSiteConfig = options.getCoreSiteConfig();
    PipelinesConfig config =
        ConfigFactory.getInstance(
                hdfsSiteConfig, coreSiteConfig, options.getProperties(), PipelinesConfig.class)
            .get();

    DateComponentOrdering dateComponentOrdering =
        options.getDefaultDateFormat() == null
            ? config.getDefaultDateFormat()
            : options.getDefaultDateFormat();

    FsUtils.deleteInterpretIfExist(
        hdfsSiteConfig, coreSiteConfig, targetPath, datasetId, attempt, types);

    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.VERBATIM_TO_INTERPRETED.name());

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createVerbatimToInterpretedMetrics();
    SerializableConsumer<String> incMetricFn = metrics::incMetric;

    log.info("Creating pipelines transforms");
    // Core
    MetadataTransform metadataTransform =
        MetadataTransform.builder()
            .clientSupplier(MetadataServiceClientFactory.getInstanceSupplier(config))
            .attempt(attempt)
            .endpointType(endPointType)
            .create()
            .counterFn(incMetricFn);

    BasicTransform basicTransform =
        BasicTransform.builder()
            .keygenServiceSupplier(KeygenServiceFactory.getInstanceSupplier(config, datasetId))
            .occStatusKvStoreSupplier(OccurrenceStatusKvStoreFactory.getInstanceSupplier(config))
            .isTripletValid(tripletValid)
            .isOccurrenceIdValid(occIdValid)
            .useExtendedRecordId(useErdId)
            .create()
            .counterFn(incMetricFn);

    TaxonomyTransform taxonomyTransform =
        TaxonomyTransform.builder()
            .kvStoreSupplier(NameUsageMatchStoreFactory.getInstanceSupplier(config))
            .create()
            .counterFn(incMetricFn);

    GrscicollTransform grscicollTransform =
        GrscicollTransform.builder()
            .kvStoreSupplier(GrscicollLookupKvStoreFactory.getInstanceSupplier(config))
            .create()
            .counterFn(incMetricFn);

    LocationTransform locationTransform =
        LocationTransform.builder()
            .geocodeKvStoreSupplier(GeocodeKvStoreFactory.getInstanceSupplier(config))
            .create()
            .counterFn(incMetricFn);

    VerbatimTransform verbatimTransform = VerbatimTransform.create().counterFn(incMetricFn);

    TemporalTransform temporalTransform =
        TemporalTransform.builder()
            .dateComponentOrdering(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn);

    // Extension
    MeasurementOrFactTransform measurementTransform =
        MeasurementOrFactTransform.builder()
            .dateComponentOrdering(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn);

    MultimediaTransform multimediaTransform =
        MultimediaTransform.builder()
            .dateComponentOrdering(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn);

    AudubonTransform audubonTransform =
        AudubonTransform.builder()
            .dateComponentOrdering(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn);

    ImageTransform imageTransform =
        ImageTransform.builder()
            .dateComponentOrdering(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn);
    // Extra
    OccurrenceExtensionTransform occExtensionTransform =
        OccurrenceExtensionTransform.create().counterFn(incMetricFn);

    DefaultValuesTransform defaultValuesTransform =
        DefaultValuesTransform.builder()
            .clientSupplier(MetadataServiceClientFactory.getInstanceSupplier(config))
            .datasetId(datasetId)
            .create();

    // Init transforms
    basicTransform.setup();
    locationTransform.setup();
    taxonomyTransform.setup();
    grscicollTransform.setup();
    metadataTransform.setup();
    defaultValuesTransform.setup();

    try (SyncDataFileWriter<ExtendedRecord> verbatimWriter =
            createWriter(options, ExtendedRecord.getClassSchema(), verbatimTransform, id);
        SyncDataFileWriter<MetadataRecord> metadataWriter =
            createWriter(options, MetadataRecord.getClassSchema(), metadataTransform, id);
        SyncDataFileWriter<BasicRecord> basicWriter =
            createWriter(options, BasicRecord.getClassSchema(), basicTransform, id);
        SyncDataFileWriter<BasicRecord> basicInvalidWriter =
            createWriter(options, BasicRecord.getClassSchema(), basicTransform, id, true);
        SyncDataFileWriter<TemporalRecord> temporalWriter =
            createWriter(options, TemporalRecord.getClassSchema(), temporalTransform, id);
        SyncDataFileWriter<MultimediaRecord> multimediaWriter =
            createWriter(options, MultimediaRecord.getClassSchema(), multimediaTransform, id);
        SyncDataFileWriter<ImageRecord> imageWriter =
            createWriter(options, ImageRecord.getClassSchema(), imageTransform, id);
        SyncDataFileWriter<AudubonRecord> audubonWriter =
            createWriter(options, AudubonRecord.getClassSchema(), audubonTransform, id);
        SyncDataFileWriter<MeasurementOrFactRecord> measurementWriter =
            createWriter(
                options, MeasurementOrFactRecord.getClassSchema(), measurementTransform, id);
        SyncDataFileWriter<TaxonRecord> taxonWriter =
            createWriter(options, TaxonRecord.getClassSchema(), taxonomyTransform, id);
        SyncDataFileWriter<GrscicollRecord> grscicollWriter =
            createWriter(options, GrscicollRecord.getClassSchema(), grscicollTransform, id);
        SyncDataFileWriter<LocationRecord> locationWriter =
            createWriter(options, LocationRecord.getClassSchema(), locationTransform, id)) {

      // Create MetadataRecord
      MetadataRecord mdr =
          metadataTransform
              .processElement(options.getDatasetId())
              .orElseThrow(() -> new IllegalArgumentException("MetadataRecord can't be null"));
      metadataWriter.append(mdr);

      // Read DWCA and replace default values
      Map<String, ExtendedRecord> erMap =
          AvroReader.readUniqueRecords(
              hdfsSiteConfig, coreSiteConfig, ExtendedRecord.class, options.getInputPath());
      Map<String, ExtendedRecord> erExtMap = occExtensionTransform.transform(erMap);
      defaultValuesTransform.replaceDefaultValues(erExtMap);

      boolean useSyncMode = options.getSyncThreshold() > erExtMap.size();

      // Filter GBIF id duplicates
      UniqueGbifIdTransform gbifIdTransform =
          UniqueGbifIdTransform.builder()
              .executor(executor)
              .erMap(erExtMap)
              .basicTransform(basicTransform)
              .useSyncMode(useSyncMode)
              .skipTransform(useErdId)
              .build()
              .run();

      // Create interpretation function
      Consumer<ExtendedRecord> interpretAllFn =
          er -> {
            BasicRecord br = gbifIdTransform.getBrInvalidMap().get(er.getId());
            if (br == null) {
              verbatimWriter.append(er);
              temporalTransform.processElement(er).ifPresent(temporalWriter::append);
              multimediaTransform.processElement(er).ifPresent(multimediaWriter::append);
              imageTransform.processElement(er).ifPresent(imageWriter::append);
              audubonTransform.processElement(er).ifPresent(audubonWriter::append);
              measurementTransform.processElement(er).ifPresent(measurementWriter::append);
              taxonomyTransform.processElement(er).ifPresent(taxonWriter::append);
              grscicollTransform.processElement(er, mdr).ifPresent(grscicollWriter::append);
              locationTransform.processElement(er, mdr).ifPresent(locationWriter::append);
            } else {
              basicInvalidWriter.append(br);
            }
          };

      log.info("Starting interpretation...");
      // Run async writing for BasicRecords
      Stream<CompletableFuture<Void>> streamBr;
      Collection<BasicRecord> brCollection = gbifIdTransform.getBrMap().values();
      if (useSyncMode) {
        streamBr =
            Stream.of(
                CompletableFuture.runAsync(
                    () -> brCollection.forEach(basicWriter::append), executor));
      } else {
        streamBr =
            brCollection.stream()
                .map(v -> CompletableFuture.runAsync(() -> basicWriter.append(v), executor));
      }

      // Run async interpretation and writing for all records
      Stream<CompletableFuture<Void>> streamAll;
      Collection<ExtendedRecord> erCollection = erExtMap.values();
      if (useSyncMode) {
        streamAll =
            Stream.of(
                CompletableFuture.runAsync(() -> erCollection.forEach(interpretAllFn), executor));
      } else {
        streamAll =
            erCollection.stream()
                .map(v -> CompletableFuture.runAsync(() -> interpretAllFn.accept(v), executor));
      }

      // Wait for all features
      CompletableFuture[] futures =
          Stream.concat(streamBr, streamAll).toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(futures).get();

    } catch (Exception e) {
      log.error("Failed performing conversion on {}", e.getMessage());
      throw new IllegalStateException("Failed performing conversion on ", e);
    } finally {
      Shutdown.doOnExit(
          metadataTransform,
          basicTransform,
          locationTransform,
          taxonomyTransform,
          grscicollTransform,
          defaultValuesTransform);
    }

    MetricsHandler.saveCountersToTargetPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }

  /** Create an AVRO file writer */
  @SneakyThrows
  private static <T> SyncDataFileWriter<T> createWriter(
      InterpretationPipelineOptions options,
      Schema schema,
      Transform transform,
      String id,
      boolean useInvalidName) {
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, id + AVRO_EXTENSION);
    String baseName = useInvalidName ? transform.getBaseInvalidName() : transform.getBaseName();
    Path path = new Path(pathFn.apply(baseName));
    FileSystem fs =
        createParentDirectories(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), path);
    return SyncDataFileWriterBuilder.builder()
        .schema(schema)
        .codec(options.getAvroCompressionType())
        .outputStream(fs.create(path))
        .syncInterval(options.getAvroSyncInterval())
        .build()
        .createSyncDataFileWriter();
  }

  private static <T> SyncDataFileWriter<T> createWriter(
      InterpretationPipelineOptions options, Schema schema, Transform transform, String id) {
    return createWriter(options, schema, transform, id, false);
  }

  /** Closes resources only one time, before JVM shuts down */
  private static class Shutdown {

    private static volatile Shutdown instance;
    private static final Object MUTEX = new Object();

    @SneakyThrows
    private Shutdown(
        MetadataTransform mdTr,
        BasicTransform bTr,
        LocationTransform lTr,
        TaxonomyTransform tTr,
        GrscicollTransform gTr,
        DefaultValuesTransform dTr) {
      Runnable shudownHook =
          () -> {
            log.info("Closing all resources");
            mdTr.tearDown();
            bTr.tearDown();
            lTr.tearDown();
            tTr.tearDown();
            gTr.tearDown();
            dTr.tearDown();
            log.info("The resources were closed");
          };
      Runtime.getRuntime().addShutdownHook(new Thread(shudownHook));
    }

    public static void doOnExit(
        MetadataTransform mdTr,
        BasicTransform bTr,
        LocationTransform lTr,
        TaxonomyTransform tTr,
        GrscicollTransform gTr,
        DefaultValuesTransform dTr) {
      if (instance == null) {
        synchronized (MUTEX) {
          if (instance == null) {
            instance = new Shutdown(mdTr, bTr, lTr, tTr, gTr, dTr);
          }
        }
      }
    }
  }
}
