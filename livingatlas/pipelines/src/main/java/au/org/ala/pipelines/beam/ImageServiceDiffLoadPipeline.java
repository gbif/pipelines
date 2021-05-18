package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.com.bytecode.opencsv.CSVParser;
import au.org.ala.images.BatchUploadResponse;
import au.org.ala.images.ImageService;
import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.pipelines.options.ImageServicePipelineOptions;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.WsUtils;
import com.google.common.base.Objects;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
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
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.rest.client.retrofit.SyncCall;
import org.slf4j.MDC;
import retrofit2.Call;

/**
 * This pipeline is used to push images and metadata updates to the image-service.
 *
 * <p>This works by:
 *
 * <ul>
 *   <li>Downloads an extract from the image service for a dataset (using the existing
 *       /ws/exportDataset service)
 *   <li>Join this extract to Multimedia AVRO data by the URL of the image using a Full Outer join
 *   <li>Retrieve a list of Multimedia objects that have not joined to something and use this as a
 *       list of new images to be loaded
 *   <li>Compare the multimedia AVRO to the extract where a join has been successful, and compare
 *       metadata fields
 *   <li>Create two zips to be posted to image service containing 1) new images 2) metadata updates.
 *       These zipped files are then sent to image service via HTTP POST.
 * </ul>
 */
@Slf4j
public class ImageServiceDiffLoadPipeline {

  public static final int NO_OF_CSV_FIELDS = 16;
  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static final Multimedia EMPTY_MULTIMEDIA = Multimedia.newBuilder().build();
  public static final String METADATA_UPDATES_PATH = "/metadata-updates/metadata";

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

    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(
                options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getProperties())
            .get();

    // create the image service
    ImageService service = WsUtils.createClient(config.getImageService(), ImageService.class);
    String imageServiceExportPath = ImageServiceSyncPipeline.downloadImageMapping(options);

    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());

    Pipeline p = Pipeline.create(options);

    // load the CSV - Create images Key-ed on URL
    PCollection<KV<String, Multimedia>> imageServiceExportMapping =
        p.apply(TextIO.read().from(imageServiceExportPath).withCompression(Compression.GZIP))
            .apply(
                ParDo.of(
                    new DoFn<String, KV<String, Multimedia>>() {
                      @ProcessElement
                      public void processElement(
                          @Element String imageMapping, OutputReceiver<KV<String, Multimedia>> out)
                          throws Exception {

                        try {
                          final CSVParser parser = new CSVParser();
                          String[] parts = parser.parseLine(imageMapping);

                          if (parts.length == NO_OF_CSV_FIELDS) {

                            // CSV is imageID
                            // Swap so we key on URL for later grouping
                            Multimedia multimedia =
                                Multimedia.newBuilder()
                                    .setIdentifier(parts[0]) // image service ID
                                    .setAudience(parts[2])
                                    .setContributor(parts[3])
                                    .setCreated(parts[4])
                                    .setCreator(parts[5])
                                    .setDescription(parts[6])
                                    .setFormat(parts[7])
                                    .setLicense(parts[8])
                                    .setPublisher(parts[9])
                                    .setReferences(parts[10])
                                    .setRightsHolder(parts[11])
                                    .setSource(parts[12])
                                    .setTitle(parts[13])
                                    .setType(parts[14])
                                    .build();

                            // output imageURL (from source) -> multimedia
                            out.output(KV.of(StringUtils.trim(parts[1]).toLowerCase(), multimedia));
                          } else {
                            log.error("Problem line length: " + imageMapping);
                          }
                        } catch (Exception e) {
                          log.error("Problem parsing line: " + imageMapping);
                        }
                      }
                    }));

    // Load Multimedia AVRO
    // Transform multimedia AVRO to map [RecordID -> Multimedia]
    log.info("Reading multimedia for this dataset");
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    PCollection<KV<String, Multimedia>> multimediaItems =
        p.apply(multimediaTransform.read(pathFn))
            .apply(ParDo.of(new ImageServiceMultimediaToMultimediaFcn()));

    // Full Outer Join with AVRO by URL
    PCollection<KV<String, KV<Multimedia, Multimedia>>> joinedCollection =
        org.apache.beam.sdk.extensions.joinlibrary.Join.fullOuterJoin(
            multimediaItems, // multimedia AVRO - what should be stored
            imageServiceExportMapping, // CSV export from image service - what is stored
            EMPTY_MULTIMEDIA,
            EMPTY_MULTIMEDIA);

    // write output to /<DATASET-ID>/<attempt>/images/image-service-record-*.avro
    String avroPath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "images-load");

    ALAFsUtils.deleteIfExist(fs, avroPath);

    if (options.isUploadNewImages()) {
      PCollection<Multimedia> newImages =
          joinedCollection
              .apply(
                  Filter.by(
                      kv ->
                          kv.getValue().getKey().getIdentifier() != null // CSV export
                              && kv.getValue().getValue().getIdentifier() == null // AVRO
                      ))
              .apply(
                  MapElements.via(
                      new SimpleFunction<KV<String, KV<Multimedia, Multimedia>>, Multimedia>() {
                        @Override
                        public Multimedia apply(KV<String, KV<Multimedia, Multimedia>> line) {
                          return line.getValue().getKey();
                        }
                      }));

      String newImagesPath = avroPath + "/new-images/images";
      newImages.apply(
          AvroIO.write(Multimedia.class)
              .to(newImagesPath)
              .withSuffix(".avro")
              .withCodec(BASE_CODEC));
    }

    if (options.isUpdateMetadata()) {
      PCollection<Multimedia> metadataChanges =
          joinedCollection
              .apply(
                  Filter.by(
                      kv -> hasMetadataChanges(kv.getValue().getKey(), kv.getValue().getValue())))
              .apply(
                  MapElements.via(
                      new SimpleFunction<KV<String, KV<Multimedia, Multimedia>>, Multimedia>() {
                        @Override
                        public Multimedia apply(KV<String, KV<Multimedia, Multimedia>> line) {
                          return line.getValue().getKey();
                        }
                      }));

      // write to AVRO
      String metadataChangesPath = avroPath + METADATA_UPDATES_PATH;
      metadataChanges.apply(
          AvroIO.write(Multimedia.class)
              .to(metadataChangesPath)
              .withSuffix(".avro")
              .withCodec(BASE_CODEC));
    }

    // create zip to push to image service of new images
    if (options.isOutputDeletes()) {
      PCollection<String> deletedImages =
          joinedCollection
              .apply(
                  Filter.by(
                      kv ->
                          kv.getValue().getKey().getIdentifier() == null
                              && kv.getValue().getValue().getIdentifier() != null))
              .apply(
                  MapElements.via(
                      new SimpleFunction<KV<String, KV<Multimedia, Multimedia>>, String>() {
                        @Override
                        public String apply(KV<String, KV<Multimedia, Multimedia>> line) {
                          return line.getValue().getValue().getIdentifier();
                        }
                      }));

      // Output a one column file with a list of imageIDs which can be removed
      // as they are no longer referenced by the dataset. This can be used to manually
      // script deletes
      deletedImages.apply(
          TextIO.write().to(avroPath + "/deletes/deleted").withSuffix(".csv").withoutSharding());
    }

    PipelineResult pipelineResult = p.run();
    pipelineResult.waitUntilFinish();
    log.info("Pipeline complete. Outputs written to: {}", avroPath);

    // HTTP POST updates to image service
    if (options.isUploadNewImages()) {
      postToImageService(options, service, fs, avroPath + "/new-images", "new-images");
    }

    // HTTP POST updates to image service
    if (options.isUpdateMetadata()) {
      postToImageService(options, service, fs, avroPath + "/metadata-updates", "metadata-updates");
    }

    log.info("Image loading successfully synchronised.");
  }

  private static void postToImageService(
      ImageServicePipelineOptions options,
      ImageService service,
      FileSystem fs,
      String multimediaPath,
      String prefix)
      throws IOException, InterruptedException {
    log.info("Create zip file of multimedia AVRO files {}", multimediaPath);
    File file =
        createMultimediaZip(
            fs, multimediaPath, options.getDatasetId(), options.getTempLocation(), prefix);

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
    log.info("Upload complete for {}", multimediaPath);
  }

  /**
   * Check for difference in metadata. Note: format isnt checked as this is a value set by
   * image-service using a format sniffer.
   *
   * @param avroImage
   * @param serviceImage
   * @return
   */
  public static boolean hasMetadataChanges(Multimedia avroImage, Multimedia serviceImage) {
    if (avroImage.getIdentifier() != null && serviceImage.getIdentifier() != null) {

      if (!Objects.equal(clean(avroImage.getAudience()), clean(serviceImage.getAudience())))
        return true;
      if (!Objects.equal(clean(avroImage.getContributor()), clean(serviceImage.getContributor())))
        return true;
      if (!Objects.equal(clean(avroImage.getCreated()), clean(serviceImage.getCreated())))
        return true;
      if (!Objects.equal(clean(avroImage.getCreator()), clean(serviceImage.getCreator())))
        return true;
      if (!Objects.equal(clean(avroImage.getDescription()), clean(serviceImage.getDescription())))
        return true;
      if (!Objects.equal(clean(avroImage.getLicense()), clean(serviceImage.getLicense())))
        return true;
      if (!Objects.equal(clean(avroImage.getPublisher()), clean(serviceImage.getPublisher())))
        return true;
      if (!Objects.equal(clean(avroImage.getReferences()), clean(serviceImage.getReferences())))
        return true;
      if (!Objects.equal(clean(avroImage.getRightsHolder()), clean(serviceImage.getRightsHolder())))
        return true;
      if (!Objects.equal(clean(avroImage.getSource()), clean(serviceImage.getSource())))
        return true;
      if (!Objects.equal(clean(avroImage.getType()), clean(serviceImage.getType()))) return true;

      return false;
    } else {
      return false;
    }
  }

  static String clean(String str) {
    String s = StringUtils.trimToNull(str);
    if (s == null) return null;
    s = s.replaceAll("\\n", "");
    s = s.replaceAll("\\r", "");
    s = s.replaceAll("\\s{2,}", " ");
    return s;
  }

  /** Function to create KV<ImageURL,RecordID> from MultimediaRecord. */
  static class ImageServiceMultimediaToMultimediaFcn
      extends DoFn<MultimediaRecord, KV<String, Multimedia>> {

    @ProcessElement
    public void processElement(
        @Element MultimediaRecord multimediaRecord, OutputReceiver<KV<String, Multimedia>> out) {
      List<Multimedia> multimediaList = multimediaRecord.getMultimediaItems();
      multimediaList.stream()
          .forEach(
              multimedia -> {
                if (multimedia.getIdentifier() != null) {
                  out.output(KV.of(multimedia.getIdentifier().toLowerCase(), multimedia));
                }
              });
    }
  }

  public static File createMultimediaZip(
      FileSystem fs, String directoryPath, String datasetID, String tempDir, String prefix)
      throws IOException {

    RemoteIterator<LocatedFileStatus> iter =
        fs.listFiles(new org.apache.hadoop.fs.Path(directoryPath), false);

    String uploadFilePath = tempDir + "/" + prefix + "-" + datasetID + ".zip";
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
