package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.options.ImageServicePipelineOptions;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.*;
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
import org.apache.beam.sdk.values.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.io.avro.ImageServiceRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.rest.client.retrofit.SyncCall;
import org.slf4j.MDC;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.GET;
import retrofit2.http.Path;

@Slf4j
public class ImageServicePipeline {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws Exception {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "images");
    ImageServicePipelineOptions options =
        PipelinesOptionsFactory.create(ImageServicePipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.IMAGE_SERVICE_METRICS);

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "IMAGE_SERVICE");

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
   *
   * @param options
   * @throws Exception
   */
  public static void run(ImageServicePipelineOptions options) throws Exception {

      FileSystem fs =
              FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
                      .getFs(options.getInputPath());

      String outputs =
              String.join(
                      "/",
                      options.getInputPath(),
                      options.getDatasetId(),
                      options.getAttempt().toString(),
                      "images");

      ALAFsUtils.deleteIfExist(fs, outputs);


    final String imageUrl = options.getImageServiceUrl();

    String imageMappingPath = downloadImageMapping(options);

    // now lets start the pipelines
    log.info("Creating a pipeline from options");

    // read the file in
    Pipeline p = Pipeline.create(options);

    // URL -> imageID, exported from image-service
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
                          // URL, imageID
                          out.output(KV.of(parts[1], parts[0]));
                        } else {
                          log.error("Problem with line: " + imageMapping);
                        }
                      }
                    }));

    // Read multimedia AVRO
    MultimediaTransform multimediaTransform = MultimediaTransform.create();
    UnaryOperator<String> pathFn =
        t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    // URL -> recordID
    log.info("Reading multimedia for this dataset");
    PCollection<KV<String, String>> multimediaItems =
        p.apply("Read Multimedia", multimediaTransform.read(pathFn))
            .apply(ParDo.of(new MultimediaRecordFcn()));

    // For image service URLs, just convert to <recordID, imageID>
    PCollection<KV<String, String>> imageServiceUrls =
        multimediaItems
            .apply(
                Filter.by(
                    new SerializableFunction<KV<String, String>, Boolean>() {
                      public Boolean apply(KV<String, String> input) {
                        return input.getKey().startsWith("https://images.ala.org.au");
                      }
                    }))
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                    .via(
                        kv ->
                            KV.of(
                                kv.getValue(),
                                kv.getKey().substring(kv.getKey().lastIndexOf("=") + 1))));

    String hdfsPath1 =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "images",
            "debug",
            "imageServiceUrls.csv");
    imageServiceUrls
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + "," + kv.getValue()))
        .apply(TextIO.write().to(hdfsPath1).withoutSharding());

    // these are NOT image service URLs
    PCollection<KV<String, String>> nonImageServiceUrls =
        multimediaItems.apply(
            Filter.by(
                new SerializableFunction<KV<String, String>, Boolean>() {
                  public Boolean apply(KV<String, String> input) {
                    return !input.getKey().startsWith("https://images.ala.org.au");
                  }
                }));

    String hdfsPath2 =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "images",
            "debug",
            "nonImageServiceUrls.csv");
    nonImageServiceUrls
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + "," + kv.getValue()))
        .apply(TextIO.write().to(hdfsPath2).withoutSharding());

    log.info("Create join collection");
    PCollection<KV<String, String>> joinedPcollection =
        org.apache.beam.sdk.extensions.joinlibrary.Join.innerJoin(
                imageServiceExportMapping, nonImageServiceUrls)
            .apply(Values.<KV<String, String>>create());

    String hdfsPath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "images",
            "debug",
            "mapped.csv");

    System.out.println("HDFS output path: " + hdfsPath);

    String avroPath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "images",
            "image-service-record");

    // Union of image service URL and non image service
    PCollectionList.of(imageServiceUrls)
        .and(joinedPcollection)
        .apply(Flatten.<KV<String, String>>pCollections())
        //        .apply(
        //            MapElements.into(TypeDescriptors.strings())
        //                .via(kv -> kv.getKey() + "," + kv.getValue()))
        //        .apply(TextIO.write().to(hdfsPath).withoutSharding());
        .apply(ParDo.of(new ImageServiceRecordFcn()))
        .apply(
            AvroIO.write(ImageServiceRecord.class)
                .to(avroPath)
                .withSuffix(".avro")
                .withCodec(BASE_CODEC));

    PipelineResult result = p.run();
    result.waitUntilFinish();

    System.out.println("Finished");
  }

  /** Function to create ALAUUIDRecords. */
  static class MultimediaRecordFcn extends DoFn<MultimediaRecord, KV<String, String>> {

    @ProcessElement
    public void processElement(
        @Element MultimediaRecord multimediaRecord, OutputReceiver<KV<String, String>> out) {
      try {
        multimediaRecord
            .getMultimediaItems()
            .forEach(
                multimedia ->
                    out.output(KV.of(multimedia.getIdentifier(), multimediaRecord.getId())));
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  static class ImageServiceRecordFcn extends DoFn<KV<String, String>, ImageServiceRecord> {

    @ProcessElement
    public void processElement(
        @Element KV<String, String> recordIDImageID, OutputReceiver<ImageServiceRecord> out) {
      try {
        out.output(
            ImageServiceRecord.newBuilder()
                .setId(recordIDImageID.getKey())
                .setImageID(recordIDImageID.getValue())
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
   *
   * @param options
   * @throws Exception
   */
  private static String downloadImageMapping(ImageServicePipelineOptions options) throws Exception {
    final Retrofit retrofit =
        new Retrofit.Builder()
            .baseUrl(options.getImageServiceUrl())
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();

    String filePath = options.getTempLocation() + "/" + options.getDatasetId() + ".csv.gz";
    System.out.println("Output to path " + filePath);
    ImageService service = retrofit.create(ImageService.class);
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

  public static void decompressToStream(File sourceFile, OutputStream fos) throws Exception {
    try {
      // Create a file input stream to read the source file.
      FileInputStream fis = new FileInputStream(sourceFile);

      // Create a gzip input stream to decompress the source
      // file defined by the file input stream.
      GZIPInputStream gzis = new GZIPInputStream(fis);

      // Create a buffer and temporary variable used during the
      // file decompress process.
      byte[] buffer = new byte[1024];
      int length;

      // Read from the compressed source file and write the
      // decompress file.
      while ((length = gzis.read(buffer)) > 0) {
        fos.write(buffer, 0, length);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public interface ImageService {
    @GET("/ws/exportDatasetMapping/{dataResourceUid}")
    Call<ResponseBody> downloadMappingFile(@Path("dataResourceUid") String dataResourceUid);
  }
}
