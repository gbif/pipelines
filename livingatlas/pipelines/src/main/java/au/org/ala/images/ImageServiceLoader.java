package au.org.ala.images;

import au.org.ala.pipelines.options.ImageServicePipelineOptions;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.File;
import java.io.FileOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;
import org.gbif.rest.client.retrofit.SyncCall;
import org.slf4j.MDC;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.*;

/** Pushes an AVRO export to the image service. */
@Slf4j
public class ImageServiceLoader {

  public static void main(String[] args) throws Exception {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "images");
    ImageServicePipelineOptions options =
        PipelinesOptionsFactory.create(ImageServicePipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "IMAGE_SYNC");
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(ImageServicePipelineOptions options) throws Exception {

    final Retrofit retrofit =
        new Retrofit.Builder()
            .baseUrl(options.getImageServiceUrl())
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();

    ImageBatchUploadService service = retrofit.create(ImageBatchUploadService.class);

    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());

    String multimediaPath = ALAFsUtils.buildPathMultimediaUsingTargetPath(options);

    // create a zip file of multimedia/*.avro
    log.info("Building zip file to submit to image service");
    java.io.File file =
        createMultimediaZip(fs, multimediaPath, options.getDatasetId(), options.getTempLocation());

    // create RequestBody instance from file
    RequestBody requestFile = RequestBody.create(file, MediaType.parse("application/zip"));

    // MultipartBody.Part is used to send also the actual file name
    MultipartBody.Part body =
        MultipartBody.Part.createFormData("archive", file.getName(), requestFile);

    RequestBody dataResourceUid =
        RequestBody.create(options.getDatasetId(), MediaType.parse("text/plain"));

    // finally, execute the request
    log.info("Uploading to image service...");
    Call<BatchUploadResponse> call = service.upload(dataResourceUid, body);
    BatchUploadResponse batchUploadResponse = SyncCall.syncCall(call);

    if (!options.isAsyncUpload()) {
      log.info("Polling image service until complete...");
      if (!batchUploadResponse.getStatus().equals("COMPLETE")) {
        log.info(" Status " + batchUploadResponse.getStatus() + " sleeping....");
        Thread.sleep(5000);
      }
    }

    log.info("Images successfully synchronised.");
  }

  public static java.io.File createMultimediaZip(
      FileSystem fs, String directoryPath, String datasetID, String tempDir) throws Exception {

    RemoteIterator<LocatedFileStatus> iter =
        fs.listFiles(new org.apache.hadoop.fs.Path(directoryPath), false);

    File newArchive = new File(tempDir + "/multimedia" + datasetID + ".zip");

    FileOutputStream fos = new FileOutputStream(newArchive);
    ZipOutputStream zipOut = new ZipOutputStream(fos);
    while (iter.hasNext()) {
      LocatedFileStatus locatedFileStatus = iter.next();
      FSDataInputStream fis = fs.open(locatedFileStatus.getPath());
      ZipEntry zipEntry = new ZipEntry(locatedFileStatus.getPath().getName());
      zipOut.putNextEntry(zipEntry);
      byte[] bytes = new byte[1024];
      int length;
      while ((length = fis.read(bytes)) >= 0) {
        zipOut.write(bytes, 0, length);
      }
      fis.close();
    }
    zipOut.close();
    fos.close();

    return newArchive;
  }

  public interface ImageBatchUploadService {
    @Multipart
    @POST("/batch/upload")
    Call<BatchUploadResponse> upload(
        @Part("dataResourceUid") RequestBody dataResourceUid, @Part MultipartBody.Part file);
  }
}
