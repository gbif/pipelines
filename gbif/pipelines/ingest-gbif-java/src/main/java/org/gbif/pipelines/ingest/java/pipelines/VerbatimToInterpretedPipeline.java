package org.gbif.pipelines.ingest.java.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.ALL;
import static org.gbif.pipelines.ingest.java.transforms.InterpretedAvroWriter.createAvroWriter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.factory.ConfigFactory;
import org.gbif.pipelines.core.factory.FileVocabularyFactory;
import org.gbif.pipelines.core.factory.FileVocabularyFactory.VocabularyBackedTerm;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.factory.ClusteringServiceFactory;
import org.gbif.pipelines.factory.GeocodeKvStoreFactory;
import org.gbif.pipelines.factory.GrscicollLookupKvStoreFactory;
import org.gbif.pipelines.factory.KeygenServiceFactory;
import org.gbif.pipelines.factory.MetadataServiceClientFactory;
import org.gbif.pipelines.factory.NameUsageMatchStoreFactory;
import org.gbif.pipelines.factory.OccurrenceStatusKvStoreFactory;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
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
import org.gbif.pipelines.transforms.common.ExtensionFilterTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.java.DefaultValuesTransform;
import org.gbif.pipelines.transforms.java.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.java.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;
import org.gbif.rest.client.species.NameUsageMatch;
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

    List<DateComponentOrdering> dateComponentOrdering =
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

    SerializableSupplier<MetadataServiceClient> metadataServiceClientSerializableSupplier =
        MetadataServiceClientFactory.getInstanceSupplier(config);
    SerializableSupplier<KeyValueStore<SpeciesMatchRequest, NameUsageMatch>>
        nameUsageMatchServiceSupplier = NameUsageMatchStoreFactory.getInstanceSupplier(config);
    SerializableSupplier<KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse>>
        grscicollServiceSupplier = GrscicollLookupKvStoreFactory.getInstanceSupplier(config);
    SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> geocodeServiceSupplier =
        GeocodeKvStoreFactory.getInstanceSupplier(config);
    if (options.getTestMode()) {
      metadataServiceClientSerializableSupplier = null;
      nameUsageMatchServiceSupplier = null;
      grscicollServiceSupplier = null;
      geocodeServiceSupplier = null;
    }

    log.info("Creating pipelines transforms");
    // Core
    MetadataTransform metadataTransform =
        MetadataTransform.builder()
            .clientSupplier(metadataServiceClientSerializableSupplier)
            .attempt(attempt)
            .endpointType(endPointType)
            .create()
            .counterFn(incMetricFn)
            .init();

    BasicTransform basicTransform =
        BasicTransform.builder()
            .keygenServiceSupplier(KeygenServiceFactory.getInstanceSupplier(config, datasetId))
            .lifeStageLookupSupplier(
                FileVocabularyFactory.getInstanceSupplier(
                    config, hdfsSiteConfig, coreSiteConfig, VocabularyBackedTerm.LIFE_STAGE))
            .occStatusKvStoreSupplier(OccurrenceStatusKvStoreFactory.getInstanceSupplier(config))
            .isTripletValid(tripletValid)
            .isOccurrenceIdValid(occIdValid)
            .useExtendedRecordId(useErdId)
            .clusteringServiceSupplier(ClusteringServiceFactory.getInstanceSupplier(config))
            .create()
            .counterFn(incMetricFn)
            .init();

    TaxonomyTransform taxonomyTransform =
        TaxonomyTransform.builder()
            .kvStoreSupplier(nameUsageMatchServiceSupplier)
            .create()
            .counterFn(incMetricFn)
            .init();

    VerbatimTransform verbatimTransform = VerbatimTransform.create().counterFn(incMetricFn);

    GrscicollTransform grscicollTransform =
        GrscicollTransform.builder()
            .kvStoreSupplier(grscicollServiceSupplier)
            .erTag(verbatimTransform.getTag())
            .brTag(basicTransform.getTag())
            .create()
            .counterFn(incMetricFn)
            .init();

    LocationTransform locationTransform =
        LocationTransform.builder()
            .geocodeKvStoreSupplier(geocodeServiceSupplier)
            .create()
            .counterFn(incMetricFn)
            .init();

    TemporalTransform temporalTransform =
        TemporalTransform.builder()
            .orderings(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn)
            .init();

    // Extension
    MultimediaTransform multimediaTransform =
        MultimediaTransform.builder()
            .orderings(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn)
            .init();

    AudubonTransform audubonTransform =
        AudubonTransform.builder()
            .orderings(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn)
            .init();

    ImageTransform imageTransform =
        ImageTransform.builder()
            .orderings(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn)
            .init();

    // Extra
    OccurrenceExtensionTransform occExtensionTransform =
        OccurrenceExtensionTransform.create().counterFn(incMetricFn);

    ExtensionFilterTransform extensionFilterTransform =
        ExtensionFilterTransform.create(config.getExtensionsAllowedForVerbatimSet());

    DefaultValuesTransform defaultValuesTransform =
        DefaultValuesTransform.builder()
            .clientSupplier(metadataServiceClientSerializableSupplier)
            .datasetId(datasetId)
            .create()
            .init();

    try (SyncDataFileWriter<ExtendedRecord> verbatimWriter =
            createAvroWriter(options, verbatimTransform, id);
        SyncDataFileWriter<MetadataRecord> metadataWriter =
            createAvroWriter(options, metadataTransform, id);
        SyncDataFileWriter<BasicRecord> basicWriter =
            createAvroWriter(options, basicTransform, id);
        SyncDataFileWriter<BasicRecord> basicInvalidWriter =
            createAvroWriter(options, basicTransform, id, true);
        SyncDataFileWriter<TemporalRecord> temporalWriter =
            createAvroWriter(options, temporalTransform, id);
        SyncDataFileWriter<MultimediaRecord> multimediaWriter =
            createAvroWriter(options, multimediaTransform, id);
        SyncDataFileWriter<ImageRecord> imageWriter =
            createAvroWriter(options, imageTransform, id);
        SyncDataFileWriter<AudubonRecord> audubonWriter =
            createAvroWriter(options, audubonTransform, id);
        SyncDataFileWriter<TaxonRecord> taxonWriter =
            createAvroWriter(options, taxonomyTransform, id);
        SyncDataFileWriter<GrscicollRecord> grscicollWriter =
            createAvroWriter(options, grscicollTransform, id);
        SyncDataFileWriter<LocationRecord> locationWriter =
            createAvroWriter(options, locationTransform, id)) {

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
      erExtMap = extensionFilterTransform.transform(erExtMap);
      defaultValuesTransform.replaceDefaultValues(erExtMap);

      boolean useSyncMode = options.getSyncThreshold() > erExtMap.size();

      // Filter GBIF id duplicates
      UniqueGbifIdTransform gbifIdTransform =
          UniqueGbifIdTransform.builder()
              .executor(executor)
              .erMap(erExtMap)
              .basicTransformFn(basicTransform::processElement)
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
              taxonomyTransform.processElement(er).ifPresent(taxonWriter::append);
              grscicollTransform.processElement(er, br, mdr).ifPresent(grscicollWriter::append);
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
      CompletableFuture<?>[] futures =
          Stream.concat(streamBr, streamAll).toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(futures).get();

    } catch (Exception e) {
      log.error("Failed performing conversion on {}", e.getMessage());
      throw new IllegalStateException("Failed performing conversion on ", e);
    } finally {
      Shutdown.doOnExit(basicTransform, locationTransform, taxonomyTransform, grscicollTransform);
    }

    MetricsHandler.saveCountersToTargetPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }

  /** Closes resources only one time, before JVM shuts down */
  private static class Shutdown {

    private static volatile Shutdown instance;
    private static final Object MUTEX = new Object();

    @SneakyThrows
    private Shutdown(
        BasicTransform bTr, LocationTransform lTr, TaxonomyTransform tTr, GrscicollTransform gTr) {
      Runnable shudownHook =
          () -> {
            log.info("Closing all resources");
            bTr.tearDown();
            lTr.tearDown();
            tTr.tearDown();
            gTr.tearDown();
            log.info("The resources were closed");
          };
      Runtime.getRuntime().addShutdownHook(new Thread(shudownHook));
    }

    public static void doOnExit(
        BasicTransform bTr, LocationTransform lTr, TaxonomyTransform tTr, GrscicollTransform gTr) {
      if (instance == null) {
        synchronized (MUTEX) {
          if (instance == null) {
            instance = new Shutdown(bTr, lTr, tTr, gTr);
          }
        }
      }
    }
  }
}
