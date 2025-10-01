package au.org.ala.sampling;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.options.SamplingPipelineOptions;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import com.google.common.collect.Iterables;
import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/**
 * A utility to crawl the ALA layers. Requires an input csv containing lat, lng (no header) and an
 * output directory.
 */
@Slf4j
public class LayerCrawler {

  public static final String UNKNOWN_STATUS = "unknown";
  public static final String FINISHED_STATUS = "finished";
  public static final String ERROR_STATUS = "error";

  private SamplingService service;

  private Retrofit retrofit;

  ALAPipelinesConfig config;

  public static void main(String[] args) throws Exception {
    CombinedYamlConfiguration conf = new CombinedYamlConfiguration(args);
    String[] combinedArgs = conf.toArgs("general", "sampling");
    SamplingPipelineOptions options =
        PipelinesOptionsFactory.create(SamplingPipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "SAMPLING");

    LayerCrawler layerCrawler = new LayerCrawler();
    layerCrawler.run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public void run(SamplingPipelineOptions options) throws Exception {

    config =
        ALAPipelinesConfigFactory.getInstance(
                HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
                options.getProperties())
            .get();

    MDC.put("step", "SAMPLING");
    retrofit =
        new Retrofit.Builder()
            .baseUrl(config.getSamplingService().getWsUrl())
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();

    log.info("Initialising crawler....");
    service = retrofit.create(SamplingService.class);
    log.info("Initialised.");

    FileSystem fs =
        FsUtils.getFileSystem(
            HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
            options.getInputPath());

    Instant batchStart = Instant.now();

    // (re)create sampling output directories
    String sampleDownloadPath = getSampleDownloadPath(options);

    // check the lat lng export directory has been created
    String latLngExportPath = getLatLngExportPath(options);
    if (!ALAFsUtils.exists(fs, latLngExportPath)) {
      log.error(
          "LatLng export unavailable. Has LatLng export pipeline been ran ? Not available at path {}",
          latLngExportPath);
      throw new PipelinesException(
          "LatLng export unavailable. Has LatLng export pipeline been ran ? Not available:"
              + latLngExportPath);
    }

    Collection<String> latLngFiles = ALAFsUtils.listPaths(fs, latLngExportPath);
    String layerList = getRequiredLayers();

    log.info("Running sampling using lat lng files: {} ", latLngFiles.size());
    for (String inputFile : latLngFiles) {
      crawl(fs, layerList, inputFile, sampleDownloadPath);
    }

    log.info("Finished layer sampling. Downloads in CSV directory: {}", sampleDownloadPath);
    log.info("Converting downloaded sampling CSV to AVRO...");
    SamplesToAvro.run(options);
    log.info("Converted.");

    Instant batchFinish = Instant.now();

    if (!options.getKeepLatLngExports()) {
      log.info("Cleaning up lat lng export.....");
      ALAFsUtils.deleteIfExist(fs, latLngExportPath);
    } else {
      log.info("Keeping lat lng exports {}", latLngExportPath);
    }

    log.info(
        "Finished sampling for {}. Time taken {} minutes",
        options.getDatasetId() != null ? options.getDatasetId() : "all points",
        Duration.between(batchStart, batchFinish).toMinutes());
  }

  @NotNull
  private static String getLatLngExportPath(AllDatasetsPipelinesOptions options) {
    if (options.getDatasetId() == null || "all".equals(options.getDatasetId())) {
      return options.getAllDatasetsInputPath() + "/latlng";
    }
    return options.getInputPath()
        + "/"
        + options.getDatasetId()
        + "/"
        + options.getAttempt()
        + "/latlng";
  }

  @NotNull
  public static String getSampleDownloadPath(AllDatasetsPipelinesOptions options) {
    if (options.getDatasetId() == null || "all".equals(options.getDatasetId())) {
      return options.getAllDatasetsInputPath() + "/sampling/downloads";
    }
    return options.getInputPath()
        + "/"
        + options.getDatasetId()
        + "/"
        + options.getAttempt()
        + "/sampling/downloads";
  }

  @NotNull
  public static String getSampleAvroPath(AllDatasetsPipelinesOptions options) {

    if (options.getDatasetId() == null || "all".equals(options.getDatasetId())) {
      return options.getAllDatasetsInputPath()
          + "/sampling/sampling-"
          + System.currentTimeMillis()
          + ".avro";
    }
    return options.getInputPath()
        + "/"
        + options.getDatasetId()
        + "/"
        + options.getAttempt()
        + "/sampling/sampling-"
        + System.currentTimeMillis()
        + ".avro";
  }

  public LayerCrawler() {}

  public String getRequiredLayers() throws IOException {

    log.info("Retrieving layer list from sampling service");
    String layers =
        Objects.requireNonNull(service.getFields().execute().body()).stream()
            .filter(Field::getEnabled)
            .map(l -> String.valueOf(l.getId()))
            .collect(Collectors.joining(","));

    log.info("Required layer count {}", layers.split(",").length);

    return layers;
  }

  public void crawl(FileSystem fs, String layers, String inputFilePath, String outputDirectoryPath)
      throws Exception {

    // partition the coordinates into batches of N to submit
    log.info("Partitioning coordinates from file {}", inputFilePath);

    InputStream inputStream = ALAFsUtils.openInputStream(fs, inputFilePath);
    Collection<List<String>> partitioned;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      Iterables.partition(reader.lines()::iterator, config.getSamplingService().getBatchSize())
          .forEach(
              (partition) -> {
                processPartition(fs, layers, inputFilePath, outputDirectoryPath, partition);
              });
    }
  }

  @SneakyThrows
  private void processPartition(
      FileSystem fs,
      String layers,
      String inputFilePath,
      String outputDirectoryPath,
      List<String> partition) {
    log.info("Partition size (no of coordinates) : {}", partition.size());
    String coords = String.join(",", partition);

    Instant batchStart = Instant.now();

    // Submit a job to generate a join
    Response<SamplingService.Batch> submit = service.submitIntersectBatch(layers, coords).execute();
    String batchId = submit.body().getBatchId();

    String state = UNKNOWN_STATUS;
    while (!state.equalsIgnoreCase(FINISHED_STATUS) && !state.equalsIgnoreCase(ERROR_STATUS)) {
      Response<SamplingService.BatchStatus> status = service.getBatchStatus(batchId).execute();
      SamplingService.BatchStatus batchStatus = status.body();
      state = batchStatus.getStatus();

      Instant batchCurrentTime = Instant.now();

      log.info(
          "batch ID {} - status: {} - time elapses {} seconds",
          batchId,
          state,
          Duration.between(batchStart, batchCurrentTime).getSeconds());

      if (!state.equals(FINISHED_STATUS)) {
        TimeUnit.MILLISECONDS.sleep(config.getSamplingService().getBatchStatusSleepTime());
      } else {
        log.info("Downloading sampling batch {}", batchId);

        downloadFile(fs, outputDirectoryPath, batchId, batchStatus);

        String zipFilePath = outputDirectoryPath + "/" + batchId + ".zip";
        ReadableByteChannel readableByteChannel = ALAFsUtils.openByteChannel(fs, zipFilePath);
        InputStream zipInput = Channels.newInputStream(readableByteChannel);

        try (ZipInputStream zipInputStream = new ZipInputStream(zipInput)) {
          ZipEntry entry = zipInputStream.getNextEntry();
          while (entry != null) {
            log.info("Unzipping {}", entry.getName());

            String unzippedOutputFilePath = outputDirectoryPath + "/" + batchId + ".csv";
            if (!entry.isDirectory()) {
              unzipFiles(fs, zipInputStream, unzippedOutputFilePath);
            }

            zipInputStream.closeEntry();
            entry = zipInputStream.getNextEntry();
          }
        }

        // delete zip file
        ALAFsUtils.deleteIfExist(fs, zipFilePath);

        log.info("Sampling done for file {}", inputFilePath);
      }
    }

    if (state.equals(ERROR_STATUS)) {
      log.error("Unable to download batch ID {}", batchId);
      throw new PipelinesException(
          "Unable to complete sampling for dataset. Check the status of sampling service for more details");
    }
  }

  /** Download the batch file with a retries mechanism. */
  private boolean downloadFile(
      FileSystem fs,
      String outputDirectoryPath,
      String batchId,
      SamplingService.BatchStatus batchStatus) {

    for (int i = 0; i < config.getSamplingService().getDownloadRetries(); i++) {

      try {
        try (ReadableByteChannel inputChannel =
                Channels.newChannel(new URL(batchStatus.getDownloadUrl()).openStream());
            WritableByteChannel outputChannel =
                ALAFsUtils.createByteChannel(fs, outputDirectoryPath + "/" + batchId + ".zip")) {
          ByteBuffer buffer = ByteBuffer.allocate(512);
          while (inputChannel.read(buffer) != -1) {
            buffer.flip();
            outputChannel.write(buffer);
            buffer.clear();
          }
          return true;
        }

      } catch (IOException e) {
        log.info("Download for batch {} failed, retrying attempt {} of 5", batchId, i);
      }
    }
    return false;
  }

  /** Unzip the file to the path. */
  public static void unzipFiles(
      final FileSystem fs, final ZipInputStream zipInputStream, final String unzippedFilePath)
      throws IOException {
    try (BufferedOutputStream bos =
        new BufferedOutputStream(ALAFsUtils.openOutputStream(fs, unzippedFilePath))) {
      byte[] bytesIn = new byte[1024];
      int read;
      while ((read = zipInputStream.read(bytesIn)) != -1) {
        bos.write(bytesIn, 0, read);
      }
    }
  }
}
