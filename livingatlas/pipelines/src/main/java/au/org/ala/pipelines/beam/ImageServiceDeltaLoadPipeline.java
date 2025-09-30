package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.images.BatchUploadResponse;
import au.org.ala.images.ImageService;
import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.client.retrofit.SyncCall;
import au.org.ala.pipelines.options.ImageServicePipelineOptions;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.WsUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.DateParsers;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.common.parsers.date.TemporalParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.slf4j.MDC;
import retrofit2.Call;

/**
 * This pipeline is used to push images and metadata updates to the image-service.
 *
 * <p>It has several modes of operation:
 *
 * <p>1) Delta updates using dublin core `modified` field. 2) Export CSV from image-service for a
 * dataResourceUid
 *
 * <p>Pushes an AVRO export to the image service.
 */
@Slf4j
public class ImageServiceDeltaLoadPipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws Exception {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "images");
    ImageServicePipelineOptions options =
        PipelinesOptionsFactory.create(ImageServicePipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "IMAGE_LOAD");
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(ImageServicePipelineOptions options)
      throws IOException, InterruptedException {

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(hdfsConfigs, options.getProperties()).get();

    // create the image service
    ImageService service = WsUtils.createClient(config.getImageService(), ImageService.class);

    FileSystem fs = FileSystemFactory.getInstance(hdfsConfigs).getFs(options.getInputPath());

    // create a zip file of multimedia/*.avro
    log.info("Building zip file to submit to image service");
    String multimediaPath;

    if (options.getModifiedWindowTimeInDays() > 0L) {
      log.info("Building delta of multimedia files");
      multimediaPath = createMultimediaDelta(options, fs);
    } else {
      log.info(
          "NOT building delta of multimedia files. Will send interpreted/multimedia directory");
      multimediaPath = ALAFsUtils.buildPathMultimediaUsingTargetPath(options);
    }

    // create zip file
    log.info("Create zip file of multimedia AVRO files");
    java.io.File file =
        createMultimediaZip(fs, multimediaPath, options.getDatasetId(), options.getTempLocation());

    // create RequestBody instance from file
    log.info("Prepare request to image service");
    RequestBody requestFile = RequestBody.create(file, MediaType.parse("application/zip"));
    MultipartBody.Part body =
        MultipartBody.Part.createFormData("archive", file.getName(), requestFile);
    RequestBody dataResourceUid =
        RequestBody.create(options.getDatasetId(), MediaType.parse("text/plain"));

    // finally, execute the request
    log.info("Uploading to image service...");
    Call<BatchUploadResponse> call = service.upload(dataResourceUid, body);
    BatchUploadResponse batchUploadResponse = SyncCall.syncCall(call);
    log.info("Response received");

    if (!options.isAsyncUpload()) {
      log.info("Polling image service until complete...");
      if (!batchUploadResponse.getStatus().equals("COMPLETE")) {
        log.info("Status " + batchUploadResponse.getStatus() + " sleeping....");
        TimeUnit.MILLISECONDS.sleep(options.getSleepTimeInMillis());
      }
    } else {
      log.info("Async response received. Check image service dashboard to monitor progress");
    }

    log.info("Image successfully synchronised.");
  }

  /**
   * Create a delta of multimedia records using the <code>
   * ImageServicePipelineOptions.modifiedWindowTimeInDays</code> to determine the length to of the
   * delta. Note: this is reliant on a `modified` field being supplied in the data.
   */
  public static String createMultimediaDelta(ImageServicePipelineOptions options, FileSystem fs) {

    // read the file in
    Pipeline p = Pipeline.create(options);

    // Read multimedia AVRO
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();

    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, ALL_AVRO);

    log.info("Reading multimedia for this dataset");
    PCollection<KV<String, MultimediaRecord>> pt1 =
        p.apply("Read Multimedia", multimediaTransform.read(pathFn))
            .apply("Map multimedia to KV", multimediaTransform.toKv());

    log.info("Reading temporal data for this dataset");
    PCollection<KV<String, TemporalRecord>> pt2 =
        p.apply("Read temporal", temporalTransform.read(pathFn))
            .apply("Map Verbatim to KV", temporalTransform.toKv());

    log.info("Grouping multimedia and tnmporal");
    PCollection<KV<String, CoGbkResult>> result =
        KeyedPCollectionTuple.of("multimedia", pt1)
            .and("temporal", pt2)
            .apply(CoGroupByKey.create());

    log.info("Calculating earliest date to use");
    final LocalDate earliestDate =
        LocalDate.now().minus(options.getModifiedWindowTimeInDays(), ChronoUnit.DAYS);

    log.info("Earliest modified date to use will be: {}", earliestDate.toString());
    PCollection<MultimediaRecord> delta =
        result.apply(
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, MultimediaRecord>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    KV<String, CoGbkResult> e = c.element();
                    MultimediaRecord multimedia = e.getValue().getOnly("multimedia", null);
                    if (multimedia != null) {
                      TemporalRecord temporal = e.getValue().getOnly("temporal");
                      String modifiedDate = temporal.getModified();
                      if (modifiedDate != null) {
                        TemporalParser parser = DateParsers.defaultTemporalParser();
                        ParseResult<TemporalAccessor> parsed = parser.parse(modifiedDate);
                        Optional<LocalDate> date =
                            Optional.ofNullable(
                                    TemporalAccessorUtils.toEarliestLocalDateTime(
                                        parsed.getPayload(), false))
                                .map(LocalDateTime::toLocalDate);
                        if (date.isPresent() && date.get().isAfter(earliestDate)) {
                          // write it out
                          c.output(multimedia);
                        }
                      } else {
                        // write it out
                        c.output(multimedia);
                      }
                    }
                  }
                }));

    String deltaPath = PathBuilder.buildDatasetAttemptPath(options, "multimedia-delta", false);
    ALAFsUtils.deleteIfExist(fs, deltaPath);

    log.info("Writing delta to {}", deltaPath);
    delta.apply(
        AvroIO.write(MultimediaRecord.class)
            .to(deltaPath + "/delta")
            .withSuffix(".avro")
            .withCodec(BASE_CODEC));

    PipelineResult pipelineResult = p.run();
    pipelineResult.waitUntilFinish();

    return deltaPath;
  }

  public static java.io.File createMultimediaZip(
      FileSystem fs, String directoryPath, String datasetID, String tempDir) throws IOException {

    RemoteIterator<LocatedFileStatus> iter =
        fs.listFiles(new org.apache.hadoop.fs.Path(directoryPath), false);

    String uploadFilePath = tempDir + "/multimedia-" + datasetID + ".zip";
    log.info("Creating zip for upload at path: " + uploadFilePath);
    File newArchive = new File(uploadFilePath);

    try (FileOutputStream fos = new FileOutputStream(newArchive);
        ZipOutputStream zipOut = new ZipOutputStream(fos)) {
      while (iter.hasNext()) {
        LocatedFileStatus locatedFileStatus = iter.next();
        try (FSDataInputStream fis = fs.open(locatedFileStatus.getPath())) {
          ZipEntry zipEntry = new ZipEntry(locatedFileStatus.getPath().getName());
          zipOut.putNextEntry(zipEntry);
          byte[] bytes = new byte[1024];
          int length;
          while ((length = fis.read(bytes)) >= 0) {
            zipOut.write(bytes, 0, length);
          }
        }
      }
    }

    return newArchive;
  }
}
