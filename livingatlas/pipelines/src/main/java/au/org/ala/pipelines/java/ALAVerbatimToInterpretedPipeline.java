package au.org.ala.pipelines.java;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.ALL;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.cache.ALACollectionKVStoreFactory;
import au.org.ala.kvs.cache.ALANameCheckKVStoreFactory;
import au.org.ala.kvs.cache.ALANameMatchKVStoreFactory;
import au.org.ala.kvs.cache.GeocodeKvStoreFactory;
import au.org.ala.kvs.cache.RecordedByKVStoreFactory;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.transforms.ALAAttributionTransform;
import au.org.ala.pipelines.transforms.ALABasicTransform;
import au.org.ala.pipelines.transforms.ALADefaultValuesTransform;
import au.org.ala.pipelines.transforms.ALAMetadataTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.transforms.ALATemporalTransform;
import au.org.ala.pipelines.transforms.LocationTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.core.io.SyncDataFileWriterBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.factory.FileVocabularyFactory;
import org.gbif.pipelines.factory.OccurrenceStatusKvStoreFactory;
import org.gbif.pipelines.io.avro.ALAAttributionRecord;
import org.gbif.pipelines.io.avro.ALAMetadataRecord;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.java.OccurrenceExtensionTransform;
import org.slf4j.MDC;

/**
 * WARNING - this is not suitable for use for archives over 500k records due to in-memory cache use.
 *
 * <p>Pipeline sequence:
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
 *      {@link org.gbif.pipelines.io.avro.ALATaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.ALAAttributionRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord}
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.ALAVerbatimToInterpretedPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.ALAVerbatimToInterpretedPipeline \
 * --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 * --attempt=1 \
 * --interpretationTypes=ALL \
 * --targetPath=/path \
 * --inputPath=/path/verbatim.avro \
 * --properties=/path/pipelines.properties \
 * --useExtendedRecordId=true
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAVerbatimToInterpretedPipeline {
  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;

  public static void main(String[] args) throws IOException {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "interpret");
    run(combinedArgs);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(String[] args) {
    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, args);
    options.setMetaFileName(ValidationUtils.INTERPRETATION_METRICS);
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
    boolean verbatimAvroAvailable = ValidationUtils.isVerbatimAvroAvailable(options);
    if (!verbatimAvroAvailable) {
      log.warn("Verbatim AVRO not available for {}", options.getDatasetId());
      return;
    }

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    Set<String> types = Collections.singleton(ALL.name());
    String targetPath = options.getTargetPath();
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(hdfsConfigs, options.getProperties()).get();

    // check collectory metadata available
    ALACollectoryMetadata m =
        ALAAttributionKVStoreFactory.create(config).get(options.getDatasetId());
    if (Objects.equals(m, ALACollectoryMetadata.EMPTY)) {
      log.error(
          "Collectory metadata no available for {}. Will not run interpretation",
          options.getDatasetId());
      return;
    }

    List<DateComponentOrdering> dateComponentOrdering =
        options.getDefaultDateFormat() == null
            ? config.getGbifConfig().getDefaultDateFormat()
            : options.getDefaultDateFormat();

    FsUtils.deleteInterpretIfExist(hdfsConfigs, targetPath, datasetId, attempt, CORE_TERM, types);

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.VERBATIM_TO_INTERPRETED.name());

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createVerbatimToInterpretedMetrics();
    SerializableConsumer<String> incMetricFn = metrics::incMetric;

    log.info("Creating pipelines transforms");

    // Core
    ALABasicTransform basicTransform =
        ALABasicTransform.builder()
            .vocabularyServiceSupplier(
                config.getGbifConfig().getVocabularyConfig() != null
                    ? FileVocabularyFactory.builder()
                        .config(config.getGbifConfig())
                        .hdfsConfigs(hdfsConfigs)
                        .build()
                        .getInstanceSupplier()
                    : null)
            .recordedByKvStoreSupplier(RecordedByKVStoreFactory.getInstanceSupplier(config))
            .occStatusKvStoreSupplier(
                OccurrenceStatusKvStoreFactory.getInstanceSupplier(config.getGbifConfig()))
            .create()
            .counterFn(incMetricFn);

    VerbatimTransform verbatimTransform = VerbatimTransform.create().counterFn(incMetricFn);
    ALATemporalTransform temporalTransform =
        ALATemporalTransform.builder()
            .orderings(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn);

    MultimediaTransform multimediaTransform =
        MultimediaTransform.builder()
            .orderings(dateComponentOrdering)
            .create()
            .counterFn(incMetricFn);
    // Extra
    OccurrenceExtensionTransform occExtensionTransform =
        OccurrenceExtensionTransform.create().counterFn(incMetricFn);

    // Collectory metadata
    ALAMetadataTransform metadataTransform =
        ALAMetadataTransform.builder()
            .dataResourceKvStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .datasetId(datasetId)
            .create();
    metadataTransform.setup();

    Optional<ALAMetadataRecord> result = metadataTransform.processElement(options.getDatasetId());
    ALAMetadataRecord mdr;
    if (result.isPresent()) {
      mdr = result.get();
    } else {
      throw new PipelinesException(
          "Unable to retrieve metadata from collectory for datasetId:" + datasetId);
    }

    // ALA specific - Attribution
    ALAAttributionTransform alaAttributionTransform =
        ALAAttributionTransform.builder()
            .collectionKvStoreSupplier(ALACollectionKVStoreFactory.getInstanceSupplier(config))
            .create();

    // ALA specific - Taxonomy
    ALATaxonomyTransform alaTaxonomyTransform =
        ALATaxonomyTransform.builder()
            .datasetId(datasetId)
            .nameMatchStoreSupplier(ALANameMatchKVStoreFactory.getInstanceSupplier(config))
            .kingdomCheckStoreSupplier(
                ALANameCheckKVStoreFactory.getInstanceSupplier("kingdom", config))
            .dataResourceStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .create();

    // ALA specific - Location
    LocationTransform locationTransform =
        LocationTransform.builder()
            .alaConfig(config)
            .countryKvStoreSupplier(GeocodeKvStoreFactory.createCountrySupplier(config))
            .stateProvinceKvStoreSupplier(GeocodeKvStoreFactory.createStateProvinceSupplier(config))
            .biomeKvStoreSupplier(GeocodeKvStoreFactory.createBiomeSupplier(config))
            .create();

    // ALA specific - Default values
    ALADefaultValuesTransform alaDefaultValuesTransform =
        ALADefaultValuesTransform.builder()
            .datasetId(datasetId)
            .dataResourceKvStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .create();

    basicTransform.setup();
    temporalTransform.setup();
    locationTransform.setup();
    alaTaxonomyTransform.setup();
    alaAttributionTransform.setup();
    multimediaTransform.setup();

    log.info("Creating writers");
    try (SyncDataFileWriter<ExtendedRecord> verbatimWriter =
            createWriter(options, ExtendedRecord.getClassSchema(), verbatimTransform, id);
        SyncDataFileWriter<BasicRecord> basicWriter =
            createWriter(options, BasicRecord.getClassSchema(), basicTransform, id);
        SyncDataFileWriter<TemporalRecord> temporalWriter =
            createWriter(options, TemporalRecord.getClassSchema(), temporalTransform, id);
        SyncDataFileWriter<MultimediaRecord> multimediaWriter =
            createWriter(options, MultimediaRecord.getClassSchema(), multimediaTransform, id);

        // ALA specific
        SyncDataFileWriter<LocationRecord> locationWriter =
            createWriter(options, LocationRecord.getClassSchema(), locationTransform, id);
        SyncDataFileWriter<ALATaxonRecord> alaTaxonWriter =
            createWriter(options, ALATaxonRecord.getClassSchema(), alaTaxonomyTransform, id);
        SyncDataFileWriter<ALAAttributionRecord> alaAttributionWriter =
            createWriter(
                options, ALAAttributionRecord.getClassSchema(), alaAttributionTransform, id)) {

      log.info("Creating metadata record");

      // Read DWCA and replace default values
      log.info("Reading Verbatim into erMap");
      Map<String, ExtendedRecord> erMap =
          AvroReader.readUniqueRecords(hdfsConfigs, ExtendedRecord.class, options.getInputPath());

      log.info("Reading DwcA - extension transform");
      Map<String, ExtendedRecord> erExtMap = occExtensionTransform.transform(erMap);
      alaDefaultValuesTransform.replaceDefaultValues(erExtMap);

      boolean useSyncMode = options.getSyncThreshold() > erExtMap.size();

      // Create interpretation function
      log.info("Create interpretation function");
      Consumer<ExtendedRecord> interpretAllFn =
          er -> {
            verbatimWriter.append(er);
            basicTransform.processElement(er).ifPresent(basicWriter::append);
            temporalTransform.processElement(er).ifPresent(temporalWriter::append);
            multimediaTransform.processElement(er).ifPresent(multimediaWriter::append);
            // ALA specific
            locationTransform.processElement(er).ifPresent(locationWriter::append);
            alaTaxonomyTransform.processElement(er).ifPresent(alaTaxonWriter::append);
            alaAttributionTransform.processElement(er, mdr).ifPresent(alaAttributionWriter::append);
          };

      // Run async interpretation and writing for all records
      log.info("Run async writing for all records");
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
      log.info("Wait for all features");
      CompletableFuture<?>[] futures = streamAll.toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(futures).get();

    } catch (Exception e) {
      log.error("Failed performing conversion on {}", e.getMessage());
      throw new IllegalStateException("Failed performing conversion on ", e);
    } finally {
      basicTransform.tearDown();
      alaTaxonomyTransform.tearDown();
      locationTransform.tearDown();
    }

    log.info("Saving metrics...");
    MetricsHandler.saveCountersToTargetPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - " + LocalDateTime.now());
  }

  /** Create an AVRO file writer */
  @SneakyThrows
  private static <T> SyncDataFileWriter<T> createWriter(
      InterpretationPipelineOptions options, Schema schema, Transform transform, String id) {
    UnaryOperator<String> pathFn =
        t ->
            PathBuilder.buildPathInterpretUsingTargetPath(
                options, CORE_TERM, t, id + AVRO_EXTENSION);
    Path path = new Path(pathFn.apply(transform.getBaseName()));
    FileSystem fs =
        FileSystemFactory.getInstance(HdfsConfigs.create(options.getHdfsSiteConfig(), null))
            .getFs(path.toString());
    fs.mkdirs(path.getParent());

    return SyncDataFileWriterBuilder.builder()
        .schema(schema)
        .codec(options.getAvroCompressionType())
        .outputStream(fs.create(path))
        .syncInterval(options.getAvroSyncInterval())
        .build()
        .createSyncDataFileWriter();
  }
}
