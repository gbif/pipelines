package au.org.ala.images;

import au.org.ala.pipelines.options.ImageServicePipelineOptions;
import au.org.ala.utils.CombinedYamlConfiguration;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.slf4j.MDC;

public class ImageSyncRunner {

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

  public static void run(ImageServicePipelineOptions options) {

    // Use "modified" to produce a subset to synchronise

    // HTTP POST TO image-service

    // Async or Sync ??

  }
}
