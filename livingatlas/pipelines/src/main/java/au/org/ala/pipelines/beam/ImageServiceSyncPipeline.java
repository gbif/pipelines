package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.images.ImageService;
import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.pipelines.options.ImageServicePipelineOptions;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import au.org.ala.utils.WsUtils;
import com.google.common.collect.ImmutableList;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.zip.GZIPInputStream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ResponseBody;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.io.avro.ImageServiceRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.rest.client.retrofit.SyncCall;
import org.slf4j.MDC;
import retrofit2.Call;

/**
 * Pipeline that takes an export from the image-service (https://images.ala.org.au) and joins this
 * with the Multimedia extension so we can associate records in AVRO files with images stored in the
 * image-service.
 *
 * <p>This pipeline contains some knowledge of image-service URLs and should join image service URLs
 * contained in the Multimedia extension to existing images or with use the original source URL to
 * join to the image-service stored artefact.
 */
@Slf4j
public class ImageServiceSyncPipeline {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws Exception {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "images");
    ImageServicePipelineOptions options =
        PipelinesOptionsFactory.create(ImageServicePipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.IMAGE_SERVICE_METRICS);

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "IMAGE_SYNC");

    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }
  /**
   * Includes the following steps:
   *
   * <p>1. Download CSV export from https://images-dev.ala.org.au/ws/exportDatasetMapping/dr123 2.
   * Extract CSV 3. Load into HDFS 4. Generate ImageServiceRecord AVRO
   */
  public static void run(ImageServicePipelineOptions options) throws IOException {

    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());

    // construct output directory path
    String outputs =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "images");

    // delete previous runs
    ALAFsUtils.deleteIfExist(fs, outputs);

    String multimedia =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "interpreted",
            "multimedia");

    if (ALAFsUtils.exists(fs, multimedia)) {

      // download the mapping from the image service
      String outputDir = downloadImageMapping(options);

      // run sync pipeline
      run(options, outputDir);

    } else {
      log.info("Interpreted multimedia directory not available for {}", options.getDatasetId());
    }
  }

  /**
   * Includes the following steps:
   *
   * <p>1. Download CSV export from https://images-dev.ala.org.au/ws/exportDatasetMapping/dr123 2.
   * Extract CSV 3. Load into HDFS 4. Generate ImageServiceRecord AVRO
   */
  public static void run(ImageServicePipelineOptions options, String imageMappingPath) {

    // now lets start the pipelines
    log.info("Creating a pipeline from options");

    Pipeline p = Pipeline.create(options);

    // Read the export file from image-service download. This is a 2 column CSV file [imageID,
    // imageURL]
    // Convert to KV of URL -> imageID
    PCollection<KV<String, String>> imageServiceExportMapping =
        p.apply(TextIO.read().from(imageMappingPath))
            .apply(
                ParDo.of(
                    new DoFn<String, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(
                          @Element String imageMapping, OutputReceiver<KV<String, String>> out) {
                        String[] parts = imageMapping.split(",");
                        if (parts.length == 2) {
                          // CSV is imageID, URL
                          // Swap so we key on URL for later grouping
                          out.output(KV.of(parts[1], parts[0]));
                        } else {
                          log.error("Problem with line: " + imageMapping);
                        }
                      }
                    }));

    // Read multimedia AVRO generated in previous interpretation step
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    // URL -> recordID
    log.info("Reading multimedia for this dataset");
    PCollection<KV<String, String>> multimediaItems =
        p.apply("Read Multimedia", multimediaTransform.read(pathFn))
            .apply("Extract URL -> RecordID map", ParDo.of(new MultimediaRecordFcn()));

    final String[] recognisedPaths = options.getRecognisedPaths().split("|");

    // For image service URLs, just convert to <recordID, imageID> but
    // Taking URLs of the form
    // https://images.ala.org.au/image/proxyImageThumbnailLarge?imageId=<UUID>
    // and substring-ing to UUID
    PCollection<KV<String, String>> imageServiceUrlsCollection =
        multimediaItems
            .apply(
                Filter.by(
                    input ->
                        Arrays.stream(recognisedPaths)
                            .anyMatch(path -> input.getKey().startsWith(path))))
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                    .via(
                        kv ->
                            KV.of(
                                kv.getValue(),
                                kv.getKey().substring(kv.getKey().lastIndexOf("=") + 1))));

    // These are NOT image service URLs i.e. they should be URLs as provided
    // by the data publisher
    PCollection<KV<String, String>> nonImageServiceUrls =
        multimediaItems.apply(
            Filter.by(
                input ->
                    Arrays.stream(recognisedPaths)
                        .noneMatch(path -> input.getKey().startsWith(path))));

    log.info("Create join collection");
    final TupleTag<String> imageServiceExportMappingTag = new TupleTag<String>() {};
    final TupleTag<String> nonImageServiceUrlsTag = new TupleTag<String>() {};

    // Merge collection values into a CoGbkResult collection.
    PCollection<KV<String, CoGbkResult>> joinedCollection =
        KeyedPCollectionTuple.of(
                imageServiceExportMappingTag,
                imageServiceExportMapping) // images extracted from image-service
            .and(nonImageServiceUrlsTag, nonImageServiceUrls) // image
            .apply(CoGroupByKey.create());

    PCollection<KV<String, String>> nonImageServiceUrlCollection =
        joinedCollection.apply(
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    KV<String, CoGbkResult> e = c.element();
                    Iterable<String> recordIDs = e.getValue().getAll(imageServiceExportMappingTag);
                    Iterable<String> imageIDs = e.getValue().getAll(nonImageServiceUrlsTag);
                    if (recordIDs.iterator().hasNext()) {
                      String recordID = recordIDs.iterator().next();
                      if (imageIDs.iterator().hasNext()) {
                        c.output(KV.of(recordID, imageIDs.iterator().next()));
                      }
                    }
                  }
                }));

    // Union of image service URL and non image service & run groupby recordID
    PCollection<KV<String, String>> combinedNotGrouped =
        PCollectionList.of(imageServiceUrlsCollection)
            .and(nonImageServiceUrlCollection)
            .apply("Flatten the non and image service", Flatten.pCollections());

    // grouped by RecordID
    PCollection<KV<String, Iterable<String>>> combined =
        combinedNotGrouped.apply("Group by RecordID", GroupByKey.create());

    // write output to /<DATASET-ID>/<attempt>/images/image-service-record-*.avro
    String avroPath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "images",
            "image-service-record");

    // write to AVRO
    combined
        .apply(ParDo.of(new ImageServiceRecordFcn()))
        .apply(
            AvroIO.write(ImageServiceRecord.class)
                .to(avroPath)
                .withSuffix(".avro")
                .withCodec(BASE_CODEC));

    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Finished");
  }

  /** Function to create KV<ImageURL,RecordID> from MultimediaRecord. */
  static class MultimediaRecordFcn extends DoFn<MultimediaRecord, KV<String, String>> {

    @ProcessElement
    public void processElement(
        @Element MultimediaRecord multimediaRecord, OutputReceiver<KV<String, String>> out) {
      try {

        List<Multimedia> list = multimediaRecord.getMultimediaItems();
        for (Multimedia multimedia : list) {
          if (multimedia.getIdentifier() != null) {
            out.output(KV.of(multimedia.getIdentifier(), multimediaRecord.getId()));
          }
        }
      } catch (Exception e) {
        log.error("Problem with record " + multimediaRecord.getId());
      }
    }
  }

  static class ImageServiceRecordFcn
      extends DoFn<KV<String, Iterable<String>>, ImageServiceRecord> {

    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<String>> recordIDImageID,
        OutputReceiver<ImageServiceRecord> out) {
      try {
        out.output(
            ImageServiceRecord.newBuilder()
                .setId(recordIDImageID.getKey())
                .setImageIDs(ImmutableList.copyOf(recordIDImageID.getValue()))
                .build());
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  /**
   * Download the mapping from the image service and write to
   *
   * <p>/PIPELINES_DIR/DATASET_ID/1/images/export.csv
   *
   * <p>for pipeline processing.
   */
  private static String downloadImageMapping(ImageServicePipelineOptions options)
      throws IOException {

    String tmpDir = options.getTempLocation() != null ? options.getTempLocation() : "/tmp";

    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(
                options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getProperties())
            .get();

    // create the image service
    ImageService service = WsUtils.createClient(config.getImageService(), ImageService.class);

    String filePath = tmpDir + "/" + options.getDatasetId() + ".csv.gz";
    log.info("Output to path " + filePath);
    Call<ResponseBody> call = service.downloadMappingFile(options.getDatasetId());

    ResponseBody responseBody = SyncCall.syncCall(call);
    InputStream inputStream = responseBody.byteStream();
    File localFile = new File(filePath);

    // download the file to local
    IOUtils.copy(inputStream, new FileOutputStream(localFile));

    // decompress to filesystem
    String hdfsPath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "images",
            "image-service-export",
            "export.csv");
    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());
    OutputStream outputStream = ALAFsUtils.openOutputStream(fs, hdfsPath);
    decompressToStream(localFile, outputStream);

    // delete the original download to avoid clogging up the local file system
    FileUtils.deleteQuietly(localFile);

    return hdfsPath;
  }

  public static void decompressToStream(File sourceFile, OutputStream fos) {
    try {
      // Create a gzip input stream to decompress the source
      // file defined by the file input stream.
      try (GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(sourceFile))) {

        // Create a buffer and temporary variable used during the
        // file decompress process.
        byte[] buffer = new byte[1024];
        int length;

        // Read from the compressed source file and write the
        // decompress file.
        while ((length = gzis.read(buffer)) > 0) {
          fos.write(buffer, 0, length);
        }
      }
    } catch (IOException e) {
      log.warn(e.getMessage(), e);
    }
  }
}
