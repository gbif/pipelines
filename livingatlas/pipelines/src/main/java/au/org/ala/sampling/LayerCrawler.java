package au.org.ala.sampling;

import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Field;
import retrofit2.http.FormUrlEncoded;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A utility to crawl the ALA layers. Requires an input csv containing lat, lng (no header)
 * and an output directory.
 */
@Slf4j
public class LayerCrawler {

    // TODO: make this configurable
    private static final String BASE_URL = "https://sampling.ala.org.au/";
    // TODO: make this configurable
    private static final String SELECTED_ELS  = "el674,el874,el774,el715,el1020,el713,el893,el598,el1006,el996,el814,el881,el1081,el705,el725,el1038,el728,el882,el889,el843,el726,el747,el793,el891,el737,el894,el1011,el720,el887,el708,el899,el681,el718,el766,el788,el810,el890,el830,el673,el898,el663,el668,el730,el669,el1019,el729,el1023,el721,el865,el879,el683,el680,el867,el892,el740,el816,el711,el845,el1003,el1078,el1079,el862,el591,el860,el866,el886,el995,el819,el772,el1013,el1073,el836,el838,el1007,el1040,el586,el704,el878,el1021,el1037,el1077,el670,el827,el1017,el1055,el876,el682,el746,el760,el787,el844,el1010,el2119,el1012,el719,el870,el672,el789,el948,el1036";
    // TODO: make this configurable
    public static final int BATCH_SIZE = 25000;
    // TODO: make this configurable
    public static final int BATCH_STATUS_SLEEP_TIME = 1000;
    // TODO: make this configurable
    public static final int DOWNLOAD_RETRIES = 5;
    public static final String UNKNOWN_STATUS = "unknown";
    public static final String FINISHED_STATUS = "finished";
    public static final String ERROR_STATUS = "error";

    SamplingService service;

    private static Retrofit retrofit =
            new Retrofit.Builder()
                    .baseUrl(BASE_URL)
                    .addConverterFactory(JacksonConverterFactory.create())
                    .validateEagerly(true)
                    .build();

    public static void main(String[] args) throws Exception  {
        String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "sample");
        InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(combinedArgs);
        run(options);
    }

    public static void run(InterpretationPipelineOptions options) throws Exception {

        String baseDir = options.getInputPath();

        FileSystem fs = FsUtils.getFileSystem(options.getHdfsSiteConfig(),  options.getCoreSiteConfig(), "/");

        if (options.getDatasetId() != null) {

            String dataSetID = options.getDatasetId();

            Instant batchStart = Instant.now();

            //list file in directory
            LayerCrawler lc = new LayerCrawler();

            //delete existing sampling output
            String samplingDir = baseDir + "/" + dataSetID + "/" + options.getAttempt() + "/sampling";
            FsUtils.deleteIfExist(options.getHdfsSiteConfig(),  options.getCoreSiteConfig(), samplingDir);

            //(re)create sampling output directories
            String sampleDownloadPath = baseDir + "/" + dataSetID + "/" + options.getAttempt() + "/sampling/downloads";

            //check the lat lng export directory has been created
            String latLngExportPath = baseDir +  "/" + dataSetID + "/" + options.getAttempt() + "/latlng";
            if (!ALAFsUtils.exists(fs, latLngExportPath)){
                log.error("LatLng export unavailable. Has LatLng export pipeline been ran ? Not available at path {}", latLngExportPath);
                throw new RuntimeException("LatLng export unavailable. Has LatLng export pipeline been ran ? Not available:" + latLngExportPath);
            }

            Collection<String> latLngFiles = ALAFsUtils.listPaths(fs, latLngExportPath);
            String layerList = lc.getRequiredLayers();

            for (Iterator<String> i = latLngFiles.iterator(); i.hasNext(); ) {
                String inputFile = i.next();
                lc.crawl(fs, layerList, inputFile, sampleDownloadPath);
            }
            Instant batchFinish = Instant.now();

            log.info("Finished sampling for {}. Time taken {} minutes", dataSetID, Duration.between(batchStart, batchFinish).toMinutes());
        } else  {

//            Instant batchStart = Instant.now();
//            //list file in directory
//            LayerCrawler lc = new LayerCrawler();
//
//            File samples = new File(options.getTargetPath() + "/sampling/downloads");
//            FileUtils.forceMkdir(samples);
//
//            Stream<File> latLngFiles = Stream.of(
//                    new File(options.getTargetPath() + "/latlng/").listFiles()
//            );
//
//            String layerList = lc.getRequiredLayers();
//
//            for (Iterator<File> i = latLngFiles.iterator(); i.hasNext(); ) {
//                File inputFile = i.next();
////                lc.crawl(fs, layerList, inputFile, samples);
//            }
//            Instant batchFinish = Instant.now();
//
//            log.info("Finished sampling for complete lat lng export. Time taken {} minutes", Duration.between(batchStart, batchFinish).toMinutes());
        }
    }

    public LayerCrawler(){
        log.info("Initialising crawler....");
        this.service = retrofit.create(SamplingService.class);
        log.info("Initialised.");
    }

    public String getRequiredLayers() throws Exception {

        log.info("Retrieving layer list from sampling service");
        List<String> requiredEls = Arrays.asList(SELECTED_ELS.split(","));
        String layers = service.getLayers().execute().body().stream()
                .filter(l -> l.getEnabled())
                .map(l -> String.valueOf(l.getId()))
//                .filter(s -> s.startsWith("cl") || requiredEls.contains(s))
                .collect(Collectors.joining(","));

        log.info("Required layer count {}",  layers.split(",").length);

        return layers;
    }


    public void crawl(FileSystem fs, String layers, String inputFilePath, String outputDirectoryPath) throws Exception {

        // partition the coordinates into batches of N to submit
        log.info("Partitioning coordinates from file {}", inputFilePath);

        InputStream inputStream = ALAFsUtils.openInputStream(fs, inputFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        Collection<List<String>> partitioned = partition(reader.lines(), BATCH_SIZE);

        for (List<String> partition : partitioned) {

            log.info("Partition size (no of coordinates) : {}", partition.size());
            String coords = String.join(",", partition);

            Instant batchStart = Instant.now();

            // Submit a job to generate a join
            Response<SamplingService.Batch> submit = service.submitIntersectBatch(layers, coords).execute();
            String batchId = submit.body().getBatchId();

            String state = UNKNOWN_STATUS;
            while (!state.equalsIgnoreCase(FINISHED_STATUS)  && !state.equalsIgnoreCase(ERROR_STATUS)) {
                Response<SamplingService.BatchStatus> status = service.getBatchStatus(batchId).execute();
                SamplingService.BatchStatus batchStatus = status.body();
                state = batchStatus.getStatus();

                Instant batchCurrentTime = Instant.now();

                log.info("batch ID {} - status: {} - time elapses {} seconds", batchId, state, Duration.between(batchStart, batchCurrentTime).getSeconds());

                if (!state.equals(FINISHED_STATUS)) {
                    Thread.sleep(BATCH_STATUS_SLEEP_TIME);
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

                    //delete zip file
                    ALAFsUtils.deleteIfExist(fs, zipFilePath);

                    log.info("Sampling done for file {}", inputFilePath);
                }
            }

            if (state.equals(ERROR_STATUS)){
                log.error("Unable to download batch ID {}", batchId);
                throw new RuntimeException("Unable to complete sampling for dataset. Check the status of sampling service for more details");
            }
        }
    }

    /**
     * Download the batch file with a retries mechanism.
     *
     * @param batchId
     * @param batchStatus
     * @return
     * @throws IOException
     */
    private boolean downloadFile(FileSystem fs, String outputDirectoryPath, String batchId, SamplingService.BatchStatus batchStatus) throws IOException {

        for (int i = 0; i < DOWNLOAD_RETRIES; i++) {

            try {
                try (
                        ReadableByteChannel inputChannel = Channels.newChannel(new URL(batchStatus.getDownloadUrl()).openStream());
                        WritableByteChannel outputChannel = ALAFsUtils.createByteChannel(fs, outputDirectoryPath + "/" + batchId + ".zip");
                ) {
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
                continue;
            }
        }
        return false;
    }

    /**
     * Simple client to the ALA sampling service.
     */
    private interface SamplingService {

        /**
         * Return an inventory of layers in the ALA spatial portal
         */
        @GET("sampling-service/fields")
        Call<List<Layer>> getLayers();

        /**
         * Return an inventory of layers in the ALA spatial portal
         */
        @GET("sampling-service/intersect/batch/{id}")
        Call<BatchStatus> getBatchStatus(@Path("id") String id);

        /**
         * Trigger a job to run the intersection and return the job key to poll.
         * @param layerIds The layers of interest in comma separated form
         * @param coordinatePairs The coordinates in lat,lng,lat,lng... format
         * @return The batch submited
         */
        @FormUrlEncoded
        @POST("sampling-service/intersect/batch")
        Call<Batch> submitIntersectBatch(@Field("fids") String layerIds,
                                         @Field("points") String coordinatePairs);

        @JsonIgnoreProperties(ignoreUnknown = true)
        class Layer {

            private String id;
            private Boolean enabled;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }


            public Boolean getEnabled() {
                return enabled;
            }

            public void setEnabled(Boolean enabled) {
                this.enabled = enabled;
            }
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        class Batch {
            private String batchId;

            public String getBatchId() {
                return batchId;
            }

            public void setBatchId(String batchId) {
                this.batchId = batchId;
            }
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        class BatchStatus {
            private String status;
            private String downloadUrl;

            public String getStatus() {
                return status;
            }

            public void setStatus(String status) {
                this.status = status;
            }

            public String getDownloadUrl() {
                return downloadUrl;
            }

            public void setDownloadUrl(String downloadUrl) {
                this.downloadUrl = downloadUrl;
            }
        }
    }

    /**
     * Util to partition a stream into fixed size windows.
     * See https://e.printstacktrace.blog/divide-a-list-to-lists-of-n-size-in-Java-8/
     */
    private static <T> Collection<List<T>> partition(Stream<T> stream, int size) {
        final AtomicInteger counter = new AtomicInteger(0);
        return stream
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / size))
                .values();
    }

    /**
     * Unzip the file to the path.
     */
    public static void unzipFiles(final FileSystem fs, final ZipInputStream zipInputStream, final String unzippedFilePath) throws IOException {
        try (BufferedOutputStream bos = new BufferedOutputStream(ALAFsUtils.openOutputStream(fs, unzippedFilePath))) {
            byte[] bytesIn = new byte[1024];
            int read = 0;
            while ((read = zipInputStream.read(bytesIn)) != -1) {
                bos.write(bytesIn, 0, read);
            }
        }
    }
}