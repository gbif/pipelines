package au.org.ala.sampling;

import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.slf4j.MDC;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Field;
import retrofit2.http.FormUrlEncoded;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;

/**
 * A utility to crawl the ALA layers. Requires an input csv containing lat, lng (no header) and an
 * output directory.
 */
@Slf4j
public class LayerCrawler {

  private static Integer batchSize;
  private static Integer batchStatusSleepTime;
  private static Integer downloadRetries;
  public static final String UNKNOWN_STATUS = "unknown";
  public static final String FINISHED_STATUS = "finished";
  public static final String ERROR_STATUS = "error";

  private final SamplingService service;

  private static Retrofit retrofit;

  public static void main(String[] args) throws Exception {
    CombinedYamlConfiguration conf = new CombinedYamlConfiguration(args);
    String[] combinedArgs = conf.toArgs("general", "sample");
    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.createInterpretation(combinedArgs);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "SAMPLING");

    init(conf);
    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void init(CombinedYamlConfiguration conf) {
    String baseUrl = (String) conf.get("layerCrawler.baseUrl");
    batchSize = (Integer) conf.get("layerCrawler.batchSize");
    batchStatusSleepTime = (Integer) conf.get("layerCrawler.batchStatusSleepTime");
    downloadRetries = (Integer) conf.get("layerCrawler.downloadRetries");
    log.info("Using {} service", baseUrl);
    retrofit =
        new Retrofit.Builder()
            .baseUrl(baseUrl)
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();
  }

  public static void run(InterpretationPipelineOptions options) throws Exception {

    String baseDir = options.getInputPath();

    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getTargetPath());

    if (options.getDatasetId() != null) {

      String dataSetID = options.getDatasetId();

      Instant batchStart = Instant.now();

      // list file in directory
      LayerCrawler lc = new LayerCrawler();

      // delete existing sampling output
      String samplingDir = baseDir + "/" + dataSetID + "/" + options.getAttempt() + "/sampling";
      FsUtils.deleteIfExist(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), samplingDir);

      // (re)create sampling output directories
      String sampleDownloadPath =
          baseDir + "/" + dataSetID + "/" + options.getAttempt() + "/sampling/downloads";

      // check the lat lng export directory has been created
      String latLngExportPath = baseDir + "/" + dataSetID + "/" + options.getAttempt() + "/latlng";
      if (!ALAFsUtils.exists(fs, latLngExportPath)) {
        log.error(
            "LatLng export unavailable. Has LatLng export pipeline been ran ? Not available at path {}",
            latLngExportPath);
        throw new RuntimeException(
            "LatLng export unavailable. Has LatLng export pipeline been ran ? Not available:"
                + latLngExportPath);
      }

      Collection<String> latLngFiles = ALAFsUtils.listPaths(fs, latLngExportPath);
      String layerList = lc.getRequiredLayers();

      for (String inputFile : latLngFiles) {
        lc.crawl(fs, layerList, inputFile, sampleDownloadPath);
      }
      Instant batchFinish = Instant.now();

      log.info(
          "Finished sampling for {}. Time taken {} minutes",
          dataSetID,
          Duration.between(batchStart, batchFinish).toMinutes());
    }
  }

  public LayerCrawler() {
    log.info("Initialising crawler....");
    this.service = retrofit.create(SamplingService.class);
    log.info("Initialised.");
  }

  public String getRequiredLayers() throws IOException {

    log.info("Retrieving layer list from sampling service");
    String layers =
        Objects.requireNonNull(service.getLayers().execute().body()).stream()
            .filter(SamplingService.Layer::getEnabled)
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
      partitioned = partition(reader.lines(), batchSize);
    }

    for (List<String> partition : partitioned) {

      log.info("Partition size (no of coordinates) : {}", partition.size());
      String coords = String.join(",", partition);

      Instant batchStart = Instant.now();

      // Submit a job to generate a join
      Response<SamplingService.Batch> submit =
          service.submitIntersectBatch(layers, coords).execute();
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
          Thread.sleep(batchStatusSleepTime);
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
        throw new RuntimeException(
            "Unable to complete sampling for dataset. Check the status of sampling service for more details");
      }
    }
  }

  /** Download the batch file with a retries mechanism. */
  private boolean downloadFile(
      FileSystem fs,
      String outputDirectoryPath,
      String batchId,
      SamplingService.BatchStatus batchStatus) {

    for (int i = 0; i < downloadRetries; i++) {

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

  /** Simple client to the ALA sampling service. */
  private interface SamplingService {

    /** Return an inventory of layers in the ALA spatial portal */
    @GET("fields")
    Call<List<Layer>> getLayers();

    /** Return an inventory of layers in the ALA spatial portal */
    @GET("intersect/batch/{id}")
    Call<BatchStatus> getBatchStatus(@Path("id") String id);

    /**
     * Trigger a job to run the intersection and return the job key to poll.
     *
     * @param layerIds The layers of interest in comma separated form
     * @param coordinatePairs The coordinates in lat,lng,lat,lng... format
     * @return The batch submited
     */
    @FormUrlEncoded
    @POST("intersect/batch")
    Call<Batch> submitIntersectBatch(
        @Field("fids") String layerIds, @Field("points") String coordinatePairs);

    @Getter
    @Setter
    @JsonIgnoreProperties(ignoreUnknown = true)
    class Layer {

      private String id;
      private Boolean enabled;
    }

    @Getter
    @Setter
    @JsonIgnoreProperties(ignoreUnknown = true)
    class Batch {
      private String batchId;
    }

    @Getter
    @Setter
    @JsonIgnoreProperties(ignoreUnknown = true)
    class BatchStatus {
      private String status;
      private String downloadUrl;
    }
  }

  /**
   * Util to partition a stream into fixed size windows. See
   * https://e.printstacktrace.blog/divide-a-list-to-lists-of-n-size-in-Java-8/
   */
  private static <T> Collection<List<T>> partition(Stream<T> stream, int size) {
    final AtomicInteger counter = new AtomicInteger(0);
    return stream.collect(Collectors.groupingBy(it -> counter.getAndIncrement() / size)).values();
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
