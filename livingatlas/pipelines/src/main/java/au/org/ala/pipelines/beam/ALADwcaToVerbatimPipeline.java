package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.DwcaToVerbatimPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.IOException;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.slf4j.MDC;

/** Wrapper around DwcaToVerbatimPipeline to allow for Yaml config setup. */
@Slf4j
public class ALADwcaToVerbatimPipeline {

  public static void main(String[] args) throws IOException {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "dwca-avro");
    DwcaToVerbatimPipelineOptions options =
        PipelinesOptionsFactory.create(DwcaToVerbatimPipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.VERBATIM_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  //  /**
  //   * Run a load for the supplied dataset, creating a lock to prevent other load process loading
  // the
  //   * same archive.
  //   */
  //  private static void runWithLocking(DwcaToVerbatimPipelineOptions options) throws IOException {
  //    // check for a lock file - if there isn't one, create one.
  //
  //    ALAFsUtils.deleteLockFile(options);
  //
  //    boolean okToProceed = ALAFsUtils.checkAndCreateLockFile(options);
  //
  //    if (okToProceed) {
  //      MDC.put("datasetId", options.getDatasetId());
  //      MDC.put("attempt", options.getAttempt().toString());
  //      MDC.put("step", StepType.DWCA_TO_VERBATIM.name());
  //      run(options);
  //
  //      if (options.isDeleteLockFileOnExit()) {
  //        ALAFsUtils.deleteLockFile(options);
  //      }
  //
  //    } else {
  //      log.info(
  //          "Dataset {} is locked. Will not attempt to loaded. Remove lockdir to proceed,",
  //          options.getDatasetId());
  //    }
  //  }

  public static void run(DwcaToVerbatimPipelineOptions options) throws IOException {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.DWCA_TO_VERBATIM.name());

    log.info("Adding step 1: Options");
    String inputPath = options.getInputPath();

    //    boolean originalInputIsHdfs = inputPath.startsWith("hdfs://") ||

    // if inputPath is "hdfs://", then copy to local
    //    if (originalInputIsHdfs) {
    //
    //      log.info("HDFS or S3 Input path: {}", inputPath);
    //      FileSystem fs =
    //          FileSystemFactory.getInstance(options.getHdfsSiteConfig(),
    // options.getCoreSiteConfig())
    //              .getFs(inputPath);
    //
    ////      Path inputPathHdfs = new Path(inputPath);
    ////
    ////      if (!fs.exists(inputPathHdfs)) {
    ////        throw new RuntimeException("Input file not available: " + inputPath);
    ////      }
    //
    //      //      String tmpInputDir = new File(options.getTempLocation()).getParent();
    //
    //      //      FileUtils.forceMkdir(new File(tmpInputDir));
    //      String tmpLocalFilePath = options.getTempLocation() + "/" + options.getDatasetId() +
    // ".zip";
    //
    //      log.info("Copy from HDFS or S3 to local FS: {}", tmpLocalFilePath);
    //
    //      Path tmpPath = new Path(tmpLocalFilePath);
    //      fs.copyToLocalFile(false, inputPathHdfs, tmpPath, true);
    //
    //      log.info("Input file copied to path  {}", tmpLocalFilePath);
    //      inputPath = tmpLocalFilePath;
    //    } else {
    //      log.info("Non-HDFS Input path: {}", inputPath);
    //    }

    String targetPath =
        String.join(
            "/",
            options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "verbatim");

    //
    //
    //        PathBuilder.buildDatasetAttemptPath(
    //            options, PipelinesVariables.Pipeline.Conversion.FILE_NAME, false);
    String tmpPath = PathBuilder.getTempDir(options);

    log.info("Input path: {}", inputPath);
    boolean isDir = Paths.get(inputPath).toFile().isDirectory();

    log.info("Input path is isDir {}", isDir);
    DwcaIO.Read reader =
        isDir
            ? DwcaIO.Read.fromLocation(inputPath)
            : DwcaIO.Read.fromCompressed(inputPath, tmpPath);

    log.info("Adding step 2: Pipeline steps");
    Pipeline p = Pipeline.create(options);

    log.info("Write path: {}", targetPath);
    p.apply("Read from Darwin Core Archive", reader)
        .apply("Write to avro", VerbatimTransform.create().write(targetPath).withoutSharding());

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    // write metrics
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
    log.info("Pipeline has been finished");
  }
}
