package au.org.ala.pipelines.java;

import static au.org.ala.pipelines.java.ALAVerbatimToInterpretedPipeline.createWriter;
import static au.org.ala.sandbox.AvroSort.sortAvroFile;
import static au.org.ala.sandbox.AvroStream.streamUniqueRecords;
import static org.gbif.api.model.pipelines.InterpretationType.RecordType.ALL;

import au.com.bytecode.opencsv.CSVParser;
import au.org.ala.kvs.ALANameMatchConfig;
import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.cache.*;
import au.org.ala.pipelines.common.SolrFieldSchema;
import au.org.ala.pipelines.options.DwcaToSolrPipelineOptions;
import au.org.ala.pipelines.options.IndexingPipelineOptions;
import au.org.ala.pipelines.transforms.*;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.sandbox.AvroSort;
import au.org.ala.sandbox.CsvSort;
import au.org.ala.sandbox.Sampling;
import au.org.ala.utils.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.io.DwcaExtendedRecordReader;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.factory.FileVocabularyFactory;
import org.gbif.pipelines.factory.OccurrenceStatusKvStoreFactory;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.jetbrains.annotations.NotNull;

/**
 * Pipeline that is used by ALA's sandbox.
 *
 * <p>Steps in this pipeline: - DwCA -> verbatim -> interpreted -> index-record -> sampling -> solr
 *
 * <p>Notable differences from ALA's general use pipeline: - Intended to be run on a single machine
 * with local fs - Lower memory requirements - No usage of collectory - No uuid stage - No sensitive
 * stage - No clustering stage - No expert distribution stage - No jackknife stage - No annotations
 * stage - No events - No images - No multimedia - No species list - Direct to SOLR cloud (does not
 * use ZK) - No runner - No metrics output
 *
 * <p>Sampling is optional.
 *
 * <p>Coarse progress is logged at each stage with log level INFO. Some examples of "... PROGRESS:
 * message" are: <code>
 * [main] INFO  au.org.ala.pipelines.java.DwcaToSolrPipeline  - PROGRESS: extracting
 * [main] INFO  au.org.ala.pipelines.java.DwcaToSolrPipeline  - PROGRESS: indexing
 * [main] INFO  au.org.ala.pipelines.java.DwcaToSolrPipeline  - PROGRESS: finished</code>
 */
@Slf4j
public class DwcaToSolrPipeline {

  public static final String EMPTY = "EMPTY";
  public static final Integer INTERPRETATION_QUEUE_SIZE = 100;

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  private static SolrClient solrClient = null;
  private static SolrClient solrAdminClient = null;

  public static void main(String[] args) throws Exception {

    VersionInfo.print();

    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "solr");
    DwcaToSolrPipelineOptions options =
        PipelinesOptionsFactory.create(DwcaToSolrPipelineOptions.class, combinedArgs);

    long start = System.currentTimeMillis();

    log.info("PROGRESS: deleting from SOLR");
    deleteFromSolr(options);
    log.info(
        "time to delete {} from SOLR took {} seconds",
        options.getDatasetId(),
        (System.currentTimeMillis() - start) / 1000);

    run(options);

    log.info(
        "Finished {} in {} seconds",
        options.getDatasetId(),
        (System.currentTimeMillis() - start) / 1000);
    log.info("PROGRESS: finished");
  }

  private static void initSolr(DwcaToSolrPipelineOptions options) {
    if (solrClient == null) {
      // direct SOLR client, to SOLR cloud, without ZK
      solrClient =
          new ConcurrentUpdateSolrClient.Builder(
                  options.getSolrHost() + "/" + options.getSolrCollection())
              .withThreadCount(2)
              .withQueueSize(options.getSolrBatchSize())
              .build();

      solrAdminClient =
          new ConcurrentUpdateSolrClient.Builder(options.getSolrHost())
              .withThreadCount(2)
              .withQueueSize(options.getSolrBatchSize())
              .build();
    }
  }

  private static void closeSolr() {
    try {
      if (solrClient != null) {
        solrClient.close();
        solrAdminClient.close();
      }
      solrClient = null;
      solrAdminClient = null;
    } catch (IOException e) {
      log.error("Error closing SOLR clients", e);
    }
  }

  private static void deleteFromSolr(DwcaToSolrPipelineOptions options) {
    initSolr(options);

    String solrUrl =
        options.getSolrHost() + "/" + options.getSolrCollection() + "/update?commit=true";
    String query = "dataResourceUid:" + options.getDatasetId();
    try {
      URL url = new URL(solrUrl);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      String jsonInputString = "{\"delete\":{\"query\":\"" + query + "\"}}";

      try (OutputStream os = connection.getOutputStream()) {
        byte[] input = jsonInputString.getBytes("utf-8");
        os.write(input, 0, input.length);
      }

      int responseCode = connection.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        log.debug("Records deleted successfully.");
      } else {
        log.debug("Failed to delete records. Response code: " + responseCode);
      }
    } catch (Exception e) {
      log.error(
          "Error deleting data resource {} from SOLR: {}",
          options.getDatasetId(),
          e.getMessage(),
          e);
    }

    closeSolr();
  }

  public static void run(DwcaToSolrPipelineOptions options) throws IOException {
    try {
      initSolr(options);

      // Step 1: dwca -> verbatim
      log.info("PROGRESS: extracting");
      long start = System.currentTimeMillis();
      dwcaToVerbatim(options);
      log.info("dwcaToVerbatim took {} seconds", (System.currentTimeMillis() - start) / 1000);

      // Step 2: verbatim -> interpreted
      log.info("PROGRESS: interpreting");
      start = System.currentTimeMillis();
      verbatimToInterpreted(options);
      log.info(
          "verbatimToInterpreted took {} seconds", (System.currentTimeMillis() - start) / 1000);

      // Step 3: interpreted -> index-record
      log.info("PROGRESS: preparing to index");
      start = System.currentTimeMillis();
      interpretedToIndexRecord(options);
      log.info(
          "interpretedToIndexRecord took {} seconds", (System.currentTimeMillis() - start) / 1000);

      // Step 4: index-record -> sampling
      if (options.getIncludeSampling()) {
        log.info("PROGRESS: intersecting");
        start = System.currentTimeMillis();
        indexRecordToSampling(options);
        log.info(
            "indexRecordToSampling took {} seconds", (System.currentTimeMillis() - start) / 1000);
      }

      // Step 5: index-record + sampling -> solr
      log.info("PROGRESS: indexing");
      start = System.currentTimeMillis();
      indexRecordToSolr(options);
      log.info("indexRecordToSolr took {} seconds", (System.currentTimeMillis() - start) / 1000);
    } finally {
      closeSolr();
    }
  }

  // based on au.org.ala.pipelines.beam.DwcaToVerbatimPipeline
  private static void dwcaToVerbatim(DwcaToSolrPipelineOptions options) throws IOException {
    log.debug("dwcaToVerbatim step 1: Options");
    String inputPath = options.getInputPath();
    String targetPath =
        PathBuilder.buildDatasetAttemptPath(
            options, PipelinesVariables.Pipeline.Conversion.FILE_NAME, false);
    String tmpPath = PathBuilder.getTempDir(options);

    boolean isDir = Paths.get(inputPath).toFile().isDirectory();

    DwcaExtendedRecordReader reader =
        isDir
            ? DwcaExtendedRecordReader.fromLocation(inputPath)
            : DwcaExtendedRecordReader.fromCompressed(inputPath, tmpPath);

    log.debug("dwcaToVerbatim step 2: Pipeline steps");

    // create directories
    new File(targetPath + ".avro").getParentFile().mkdirs();

    // Performance: remove pipeline and run on a single thread
    DatumWriter<ExtendedRecord> writer = new SpecificDatumWriter<>(ExtendedRecord.class);
    try (DataFileWriter<ExtendedRecord> dataFileWriter = new DataFileWriter<>(writer)) {
      dataFileWriter.setCodec(org.apache.avro.file.CodecFactory.snappyCodec());
      dataFileWriter.create(ExtendedRecord.getClassSchema(), new File(targetPath + ".avro"));

      while (reader.advance()) {
        ExtendedRecord er = reader.getCurrent();
        if (er == null) {
          break;
        }

        dataFileWriter.append(er);
      }
    }
  }

  // based on au.org.ala.pipelines.beam.ALAVerbatimToInterpretedPipeline
  private static void verbatimToInterpreted(DwcaToSolrPipelineOptions options) {
    ExecutorService executor = Executors.newFixedThreadPool(options.getMaxThreadCount());
    try {
      verbatimToInterpretedRun(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  public static void verbatimToInterpretedRun(
      DwcaToSolrPipelineOptions options, ExecutorService executor) {
    boolean verbatimAvroAvailable = ValidationUtils.isVerbatimAvroAvailable(options);
    if (!verbatimAvroAvailable) {
      log.warn("Verbatim AVRO not available for {}", options.getDatasetId());
      return;
    }
    String postfix = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    Set<String> types = Collections.singleton(ALL.name());
    String targetPath = options.getTargetPath();
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(HdfsConfigs.nullConfig(), options.getProperties())
            .get();

    List<DateComponentOrdering> dateComponentOrdering =
        options.getDefaultDateFormat() == null
            ? config.getGbifConfig().getDefaultDateFormat()
            : options.getDefaultDateFormat();

    FsUtils.deleteInterpretIfExist(
        hdfsConfigs, targetPath, datasetId, attempt, DwcTerm.Occurrence, types);

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    log.debug("Creating pipelines transforms");

    // Core
    ALABasicTransform basicTransform =
        ALABasicTransform.builder()
            .vocabularyServiceSupplier(
                config.getGbifConfig().getVocabularyConfig() != null
                    ? FileVocabularyFactory.getInstanceSupplier(hdfsConfigs, config.getGbifConfig())
                    : null)
            .recordedByKvStoreSupplier(RecordedByKVStoreFactory.getInstanceSupplier(config))
            .occStatusKvStoreSupplier(
                OccurrenceStatusKvStoreFactory.getInstanceSupplier(config.getGbifConfig()))
            .create();

    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    ALATemporalTransform temporalTransform =
        ALATemporalTransform.builder().orderings(dateComponentOrdering).create();

    MultimediaTransform multimediaTransform =
        MultimediaTransform.builder().orderings(dateComponentOrdering).create();

    // Collectory metadata, remove collectory URL so it operates in sandbox mode
    if (config.getCollectory() != null) {
      config.getCollectory().setWsUrl(null);
    }
    ALAMetadataTransform metadataTransform =
        ALAMetadataTransform.builder()
            .dataResourceKvStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .datasetId(datasetId)
            .create();
    metadataTransform.setup();

    // add measurement or facts...
    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.builder().create();

    ALAMetadataRecord mdr =
        ALAMetadataRecord.newBuilder()
            .setId(options.getDatasetId())
            .setDataResourceName(options.getDatasetId())
            .setDataResourceUid(options.getDatasetId())
            .build();
    SyncDataFileWriter metadataWriter =
        createWriter(
            options,
            ALAMetadataRecord.getClassSchema(),
            metadataTransform,
            DwcTerm.Occurrence,
            postfix);
    metadataWriter.append(mdr);
    try {
      metadataWriter.close();
    } catch (IOException e) {
      throw new PipelinesException(
          "Unable to write metadata AVRO for datasetId: " + options.getDatasetId(), e);
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
            .alaNameMatchConfig(
                config.getAlaNameMatchConfig() != null
                    ? config.getAlaNameMatchConfig()
                    : new ALANameMatchConfig())
            .create();

    // ALA specific - Location
    LocationTransform locationTransform =
        LocationTransform.builder()
            .alaConfig(config)
            .countryKvStoreSupplier(GeocodeKvStoreFactory.createCountrySupplier(config))
            .stateProvinceKvStoreSupplier(GeocodeKvStoreFactory.createStateProvinceSupplier(config))
            .biomeKvStoreSupplier(GeocodeKvStoreFactory.createBiomeSupplier(config))
            .create();

    basicTransform.setup();
    temporalTransform.setup();
    locationTransform.setup();
    alaTaxonomyTransform.setup();
    alaAttributionTransform.setup();
    multimediaTransform.setup();

    log.debug("Creating writers");
    // String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
    try (SyncDataFileWriter<ExtendedRecord> verbatimWriter =
            createWriter(
                options,
                ExtendedRecord.getClassSchema(),
                verbatimTransform,
                DwcTerm.Occurrence,
                id);
        SyncDataFileWriter<BasicRecord> basicWriter =
            createWriter(
                options, BasicRecord.getClassSchema(), basicTransform, DwcTerm.Occurrence, id);
        SyncDataFileWriter<TemporalRecord> temporalWriter =
            createWriter(
                options,
                TemporalRecord.getClassSchema(),
                temporalTransform,
                DwcTerm.Occurrence,
                id);
        SyncDataFileWriter<MultimediaRecord> multimediaWriter =
            createWriter(
                options,
                MultimediaRecord.getClassSchema(),
                multimediaTransform,
                DwcTerm.Occurrence,
                id);
        SyncDataFileWriter<MeasurementOrFactRecord> measurementOrFactWriter =
            createWriter(
                options,
                MeasurementOrFactRecord.getClassSchema(),
                measurementOrFactTransform,
                DwcTerm.Occurrence,
                id);
        // ALA specific
        SyncDataFileWriter<LocationRecord> locationWriter =
            createWriter(
                options,
                LocationRecord.getClassSchema(),
                locationTransform,
                DwcTerm.Occurrence,
                id);
        SyncDataFileWriter<ALATaxonRecord> alaTaxonWriter =
            createWriter(
                options,
                ALATaxonRecord.getClassSchema(),
                alaTaxonomyTransform,
                DwcTerm.Occurrence,
                id);
        SyncDataFileWriter<ALAAttributionRecord> alaAttributionWriter =
            createWriter(
                options,
                ALAAttributionRecord.getClassSchema(),
                alaAttributionTransform,
                DwcTerm.Occurrence,
                id)) {

      log.debug("Creating metadata record");

      ExtendedRecord terminationSignal = new ExtendedRecord();
      terminationSignal.setId("TERMINATION_SIGNAL");

      // Stream DWCA (does not use collectory so no default values to replace)
      Stream<ExtendedRecord> erStream =
          streamUniqueRecords(
              hdfsConfigs,
              ExtendedRecord.class,
              options.getTargetPath() + "/" + options.getDatasetId() + "/1/verbatim.avro",
              null);

      // Create interpretation function
      log.debug("Create interpretation function");
      AtomicLong verbatimTime = new AtomicLong(0);
      AtomicLong basicTime = new AtomicLong(0);
      AtomicLong temporalTime = new AtomicLong(0);
      AtomicLong multimediaTime = new AtomicLong(0);
      AtomicLong locationTime = new AtomicLong(0);
      AtomicLong alaTaxonTime = new AtomicLong(0);
      AtomicLong alaAttributionTime = new AtomicLong(0);
      AtomicLong measurementOrFactTime = new AtomicLong(0);
      AtomicInteger timeCounter = new AtomicInteger(0);

      Consumer<ExtendedRecord> interpretAllFn =
          er -> {
            long startTime;

            startTime = System.currentTimeMillis();
            verbatimWriter.append(er);
            verbatimTime.addAndGet(System.currentTimeMillis() - startTime);

            startTime = System.currentTimeMillis();
            basicTransform.processElement(er).ifPresent(basicWriter::append);
            basicTime.addAndGet(System.currentTimeMillis() - startTime);

            startTime = System.currentTimeMillis();
            temporalTransform.processElement(er).ifPresent(temporalWriter::append);
            temporalTime.addAndGet(System.currentTimeMillis() - startTime);

            startTime = System.currentTimeMillis();
            //
            multimediaTransform.processElement(er).ifPresent(multimediaWriter::append);
            multimediaTime.addAndGet(System.currentTimeMillis() - startTime);

            // ALA specific

            startTime = System.currentTimeMillis();
            locationTransform.processElement(er).ifPresent(locationWriter::append);
            locationTime.addAndGet(System.currentTimeMillis() - startTime);

            startTime = System.currentTimeMillis();
            alaTaxonomyTransform.processElement(er).ifPresent(alaTaxonWriter::append);
            alaTaxonTime.addAndGet(System.currentTimeMillis() - startTime);

            startTime = System.currentTimeMillis();
            alaAttributionTransform.processElement(er, mdr).ifPresent(alaAttributionWriter::append);
            alaAttributionTime.addAndGet(System.currentTimeMillis() - startTime);

            startTime = System.currentTimeMillis();
            measurementOrFactTransform
                .processElement(er)
                .ifPresent(measurementOrFactWriter::append);
            measurementOrFactTime.addAndGet(System.currentTimeMillis() - startTime);

            int timeCount = timeCounter.addAndGet(1);

            // log every 1000 records
            if (timeCount % 1000 == 0) {
              log.debug(
                  "Count: {}, Verbatim: {}, Basic: {}, Temporal: {}, Multimedia: {}, Location: {}, Taxon: {}, Attribution: {}, MeasurementOrFact: {}",
                  timeCount,
                  verbatimTime.get(),
                  basicTime.get(),
                  temporalTime.get(),
                  multimediaTime.get(),
                  locationTime.get(),
                  alaTaxonTime.get(),
                  alaAttributionTime.get(),
                  measurementOrFactTime.get());
            }
          };

      // Run async interpretation and writing for all records
      log.debug("verbatimToInterpretedRun async writing for all records");
      LinkedBlockingQueue<ExtendedRecord> queue =
          new LinkedBlockingQueue<>(INTERPRETATION_QUEUE_SIZE); // Adjust the capacity as needed

      int numConsumers = options.getMaxThreadCount(); // Adjust the number of consumers as needed
      CountDownLatch latch = new CountDownLatch(numConsumers);

      // Consumer: Takes items from the queue and processes them
      Runnable consumerTask =
          () -> {
            try {
              while (true) {
                ExtendedRecord er = queue.take();

                // check for termination signal
                if (er == terminationSignal) {
                  latch.countDown();
                  Thread.currentThread().interrupt();
                  return;
                }

                interpretAllFn.accept(er);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          };

      // Run multiple consumer tasks using the executor pool
      for (int i = 0; i < numConsumers; i++) {
        executor.submit(consumerTask);
      }

      // Producer: Feeds items from erStream into the queue
      Runnable producerTask =
          () -> {
            erStream.forEach(
                er -> {
                  try {
                    queue.put(er);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                });

            // append consumer termination signal to the queue
            for (int i = 0; i < numConsumers; i++) {
              try {
                queue.put(terminationSignal);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            }
          };

      // run producer task on a new thread, then wait for it to finish
      Thread producerThread = new Thread(producerTask);
      producerThread.start();
      try {
        producerThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      // wait for all consumers to finish
      latch.await();
    } catch (Exception e) {
      log.error("Failed performing conversion on {}", e.getMessage(), e);
      throw new IllegalStateException("Failed performing conversion on ", e);
    } finally {
      basicTransform.tearDown();
      alaTaxonomyTransform.tearDown();
      locationTransform.tearDown();
    }

    log.debug("verbatimToInterpretedRun has been finished - {}", LocalDateTime.now());
  }

  // based on au.org.ala.pipelines.java.IndexRecordPipeline
  private static void interpretedToIndexRecord(IndexingPipelineOptions options) throws IOException {
    interpretedToIndexRecordRun(options);

    // Sort the result
    String indexRecordPath =
        options.getTargetPath()
            + "/"
            + options.getDatasetId()
            + "/index-record/"
            + options.getDatasetId()
            + ".avro";

    String indexRecordSortedPath =
        options.getTargetPath()
            + "/"
            + options.getDatasetId()
            + "/index-record/"
            + options.getDatasetId()
            + "-sorted.avro";

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    sortAvroFile(
        hdfsConfigs,
        IndexRecord.class,
        IndexRecord.getClassSchema(),
        indexRecordPath,
        indexRecordSortedPath);
  }

  private static void interpretedToIndexRecordRun(IndexingPipelineOptions options)
      throws IOException {

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    // get filesystem
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, options.getInputPath());

    String outputPath =
        options.getTargetPath()
            + "/"
            + options.getDatasetId()
            + "/index-record/"
            + options.getDatasetId()
            + ".avro";

    // clean previous runs
    ALAFsUtils.deleteIfExist(fs, outputPath);
    OutputStream output = fs.create(new Path(outputPath));

    final long lastLoadedDate = Time.now();
    final long lastProcessedDate = lastLoadedDate;

    log.debug("Sort input files by id");
    sortAvroFile(
        hdfsConfigs,
        ExtendedRecord.class,
        ExtendedRecord.getClassSchema(),
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/verbatim.avro",
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/verbatim-sorted.avro");
    AvroSort.AvroFileReader<ExtendedRecord> verbatimReader =
        new AvroSort.AvroFileReader<>(
            new File(PathBuilder.buildDatasetAttemptPath(options, "verbatim-sorted.avro", false)),
            ExtendedRecord.class);

    sortAvroFile(
        hdfsConfigs,
        BasicRecord.class,
        BasicRecord.getClassSchema(),
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/occurrence/basic/*",
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/basic-sorted.avro");
    AvroSort.AvroFileReader<BasicRecord> basicReader =
        new AvroSort.AvroFileReader<>(
            new File(PathBuilder.buildDatasetAttemptPath(options, "basic-sorted.avro", false)),
            BasicRecord.class);

    sortAvroFile(
        hdfsConfigs,
        TemporalRecord.class,
        TemporalRecord.getClassSchema(),
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/occurrence/temporal/*",
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/temporal-sorted.avro");
    AvroSort.AvroFileReader<TemporalRecord> temporalReader =
        new AvroSort.AvroFileReader<>(
            new File(PathBuilder.buildDatasetAttemptPath(options, "temporal-sorted.avro", false)),
            TemporalRecord.class);

    sortAvroFile(
        hdfsConfigs,
        LocationRecord.class,
        LocationRecord.getClassSchema(),
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/occurrence/location/*",
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/location-sorted.avro");
    AvroSort.AvroFileReader<LocationRecord> locationReader =
        new AvroSort.AvroFileReader<>(
            new File(PathBuilder.buildDatasetAttemptPath(options, "location-sorted.avro", false)),
            LocationRecord.class);

    sortAvroFile(
        hdfsConfigs,
        ALATaxonRecord.class,
        ALATaxonRecord.getClassSchema(),
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/occurrence/ala_taxonomy/*",
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/ala_taxonomy-sorted.avro");
    AvroSort.AvroFileReader<ALATaxonRecord> alaTaxonReader =
        new AvroSort.AvroFileReader<>(
            new File(
                PathBuilder.buildDatasetAttemptPath(options, "ala_taxonomy-sorted.avro", false)),
            ALATaxonRecord.class);

    sortAvroFile(
        hdfsConfigs,
        ALAAttributionRecord.class,
        ALAAttributionRecord.getClassSchema(),
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/occurrence/ala_attribution/*",
        options.getTargetPath() + "/" + options.getDatasetId() + "/1/ala_attribution-sorted.avro");
    AvroSort.AvroFileReader<ALAAttributionRecord> alaAttributionReader =
        new AvroSort.AvroFileReader<>(
            new File(
                PathBuilder.buildDatasetAttemptPath(options, "ala_attribution-sorted.avro", false)),
            ALAAttributionRecord.class);

    log.debug("Joining avro files...");
    // Join all records, convert into string json and IndexRequest for ES/SOLR
    Function<BasicRecord, IndexRecord> indexRequestFn =
        br -> {
          String k = br.getId();

          // Core
          ExtendedRecord er =
              verbatimReader.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());
          TemporalRecord tr =
              temporalReader.getOrDefault(k, TemporalRecord.newBuilder().setId(k).build());
          LocationRecord lr =
              locationReader.getOrDefault(k, LocationRecord.newBuilder().setId(k).build());

          // ALA sandbox specific, while currently consistent, should uniqueness be enforced?
          String uuid = UUID.randomUUID().toString();

          // ALA specific
          ALAUUIDRecord aur = ALAUUIDRecord.newBuilder().setId(k).setUuid(uuid).build();
          ALATaxonRecord atxr =
              alaTaxonReader.getOrDefault(k, ALATaxonRecord.newBuilder().setId(k).build());
          ALAAttributionRecord aar =
              alaAttributionReader.getOrDefault(
                  k, ALAAttributionRecord.newBuilder().setId(k).build());

          return IndexRecordTransform.createIndexRecord(
              br,
              tr,
              lr,
              null,
              atxr,
              er,
              aar,
              aur,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              lastLoadedDate,
              lastProcessedDate);
        };

    DatumWriter<IndexRecord> datumWriter = new GenericDatumWriter<>(IndexRecord.getClassSchema());
    try (DataFileWriter<IndexRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.setCodec(BASE_CODEC);
      dataFileWriter.create(IndexRecord.getClassSchema(), output);

      // iterate over basicReader
      BasicRecord basicRecord;
      while ((basicRecord = basicReader.next()) != null) {
        dataFileWriter.append(indexRequestFn.apply(basicRecord));
      }
    }

    // Not ideal, should be in a try, but close the readers here
    verbatimReader.close();
    basicReader.close();
    temporalReader.close();
    locationReader.close();
    alaTaxonReader.close();
    alaAttributionReader.close();

    log.debug("IndexRecordPipeline has been finished - {}", LocalDateTime.now());
  }

  // based on au.org.ala.pipelines.beam.IndexRecordToSamplingPipeline
  private static void indexRecordToSampling(DwcaToSolrPipelineOptions options) throws IOException {
    String indexRecordSortedPath =
        options.getTargetPath()
            + "/"
            + options.getDatasetId()
            + "/index-record/"
            + options.getDatasetId()
            + "-sorted.avro";

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    String outputPath =
        String.join(
            Path.SEPARATOR,
            options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString());

    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(
                HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
                options.getProperties())
            .get();

    Sampling.sample(
        hdfsConfigs,
        indexRecordSortedPath,
        outputPath,
        config.getSamplingService().getWsUrl(),
        config.getSamplingService().getBatchStatusSleepTime(),
        config.getSamplingService().getBatchSize(),
        config.getSamplingService().getDownloadRetries());
  }

  // based on au.org.ala.pipelines.beam.IndexRecordToSolrPipeline
  private static void indexRecordToSolr(DwcaToSolrPipelineOptions options) throws IOException {
    final Map<String, SolrFieldSchema> schemaFields = getSchemaFields(options);
    final List<String> dynamicFieldPrefixes = getSchemaDynamicFieldPrefixes(options);

    // Step 1: get streaming objects
    String indexRecordSortedPath =
        options.getTargetPath()
            + "/"
            + options.getDatasetId()
            + "/index-record/"
            + options.getDatasetId()
            + "-sorted.avro";
    AvroSort.AvroFileReader<IndexRecord> indexRecordReader =
        new AvroSort.AvroFileReader<>(new File(indexRecordSortedPath), IndexRecord.class);

    CSVParser csvParser = new CSVParser();

    // read the intersect-header.csv to string
    String[] header = null;
    if (options.getIncludeSampling()) {
      try (BufferedReader br =
          new BufferedReader(
              new FileReader(
                  PathBuilder.buildDatasetAttemptPath(options, "intersect-header.csv", false)))) {
        header = csvParser.parseLine(br.readLine());
      }
    }

    // read the sorted intersect file
    CsvSort.CsvQueueReader csvQueueReader =
        options.getIncludeSampling()
            ? new CsvSort.CsvQueueReader(
                new BufferedReader(
                    new FileReader(
                        PathBuilder.buildDatasetAttemptPath(
                            options, "intersect-sorted.csv", false))))
            : null;

    log.debug("IndexRecordToSolrPipeline has been started - {}", LocalDateTime.now());

    List<SolrInputDocument> batch = new ArrayList<>();
    while (indexRecordReader.hasNext()) {
      IndexRecord record = indexRecordReader.next();

      // add sampling data to the index record
      if (csvQueueReader != null
          && csvQueueReader.hasNext()
          && csvQueueReader.id.equals(record.getId())) {
        String csvLine = csvQueueReader.next();

        // parse this CSV line
        String[] row = csvParser.parseLine(csvLine);

        for (int i = 3; i < header.length && i < row.length; i++) {
          if (StringUtils.isNotEmpty(row[i])) {
            if (header[i].startsWith("c")) {
              record.getStrings().put(header[i], row[i]);
            } else {
              record.getDoubles().put(header[i], Double.parseDouble(row[i]));
            }
          }
        }
      }

      // create SOLR document
      SolrInputDocument solrInputDocument =
          IndexRecordTransform.convertIndexRecordToSolrDoc(
              record, schemaFields, dynamicFieldPrefixes);

      batch.add(solrInputDocument);

      if (batch.size() == options.getSolrBatchSize()) {
        flushBatchToSolr(batch, options);
      }
    }
    if (!batch.isEmpty()) {
      flushBatchToSolr(batch, options);
    }

    // close the readers, should be in a try
    indexRecordReader.close();

    if (csvQueueReader != null) {
      csvQueueReader.close();
    }

    log.debug("IndexRecordToSolrPipeline has been finished - {}", LocalDateTime.now());
  }

  private static void flushBatchToSolr(
      List<SolrInputDocument> batch, DwcaToSolrPipelineOptions options) {
    boolean successful = false;
    int attempts = options.getSolrRetryMaxAttempts();
    while (attempts > 0 && !successful) {
      attempts--;
      try {
        solrClient.add(batch);
        solrClient.commit(true, true);
        successful = true;
      } catch (Exception e) {
        log.error("Unable to write to SOLR: " + e.getMessage());
      }
    }

    if (!successful) {
      throw new PipelinesException("Unable to write to SOLR after retries");
    }

    batch.clear();
  }

  @NotNull
  private static Map<String, SolrFieldSchema> getSchemaFields(DwcaToSolrPipelineOptions options) {
    try {
      SchemaRequest.Fields fields = new SchemaRequest.Fields();
      SchemaResponse.FieldsResponse response =
          fields.process(solrAdminClient, options.getSolrCollection());
      Map<String, SolrFieldSchema> schema = new HashMap<String, SolrFieldSchema>();
      for (Map<String, Object> field : response.getFields()) {
        schema.put(
            (String) field.get("name"),
            new SolrFieldSchema(
                (String) field.get("type"), (boolean) field.getOrDefault("multiValued", false)));
      }
      return schema;
    } catch (Exception e) {
      throw new PipelinesException("Unable to retrieve schema fields: " + e.getMessage());
    }
  }

  @NotNull
  private static List<String> getSchemaDynamicFieldPrefixes(DwcaToSolrPipelineOptions options) {
    try {
      SchemaRequest.DynamicFields fields = new SchemaRequest.DynamicFields();
      SchemaResponse.DynamicFieldsResponse response =
          fields.process(solrAdminClient, options.getSolrCollection());
      return response.getDynamicFields().stream()
          .map(f -> f.get("name").toString().replace("*", ""))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new PipelinesException("Unable to retrieve schema fields: " + e.getMessage());
    }
  }
}
