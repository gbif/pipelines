package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.DwcaToVerbatimPipelineOptions;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.plexus.util.FileUtils;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.slf4j.MDC;

/** Thin wrapper around DwcaToVerbatimPipeline to allow for Yaml config setup. */
@Slf4j
public class ALADwcaToVerbatimPipeline {

  public static void main(String[] args) throws IOException {

    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "dwca-avro");
    DwcaToVerbatimPipelineOptions options =
        PipelinesOptionsFactory.create(DwcaToVerbatimPipelineOptions.class, combinedArgs);
    PipelinesOptionsFactory.registerHdfs(options);

    // handle run for all datasets
    if (options.getDatasetId() == null || options.getDatasetId().equalsIgnoreCase("all")) {

      log.info("Running Dwca -> Verbatim for all datasets");

      // load all datasets - return a map of <datasetId -> datasetInputPath>
      Map<String, String> datasets = ALAFsUtils.listAllDatasets(options);

      // run for all found datasets
      for (Map.Entry<String, String> datasetIDAndPath : datasets.entrySet()) {
        options.setDatasetId(datasetIDAndPath.getKey());
        options.setInputPath(datasetIDAndPath.getValue());
        runWithLocking(options);
      }

    } else {
      runWithLocking(options);
    }
  }

  /**
   * Run a load for the supplied dataset, creating a lock to prevent other load process loading the
   * same archive.
   *
   * @param options
   * @throws IOException
   */
  private static void runWithLocking(DwcaToVerbatimPipelineOptions options) throws IOException {
    // check for a lock file - if there isn't one, create one.
    boolean okToProceed = ALAFsUtils.checkAndCreateLockFile(options);

    if (okToProceed) {
      MDC.put("datasetId", options.getDatasetId());
      MDC.put("attempt", options.getAttempt().toString());
      MDC.put("step", StepType.DWCA_TO_VERBATIM.name());
      run(options);

      if (options.isDeleteLockFileOnExit()) {
        ALAFsUtils.deleteLockFile(options);
      }

    } else {
      log.info(
          "Dataset {} is locked. Will not attempt to loaded. Remove lockdir to proceed,",
          options.getDatasetId());
    }
  }

  public static void run(DwcaToVerbatimPipelineOptions options) throws IOException {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.DWCA_TO_VERBATIM.name());

    log.info("Adding step 1: Options");
    String inputPath = options.getInputPath();

    boolean originalInputIsHdfs = inputPath.startsWith("hdfs://");

    // if inputPath is "hdfs://", then copy to local
    if (originalInputIsHdfs) {

      log.info("HDFS Input path: {}", inputPath);
      FileSystem fs =
          FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
              .getFs(inputPath);

      Path inputPathHdfs = new Path(inputPath);

      if (!fs.exists(inputPathHdfs)) {
        throw new RuntimeException("Input file not available: " + inputPath);
      }

      String tmpInputDir = new File(options.getTempLocation()).getParent();

      FileUtils.forceMkdir(new File(tmpInputDir));
      String tmpLocalFilePath = tmpInputDir + "/" + options.getDatasetId() + ".zip";

      log.info("Copy from HDFS to local FS: {}", tmpLocalFilePath);

      Path tmpPath = new Path(tmpLocalFilePath);
      fs.copyToLocalFile(false, inputPathHdfs, tmpPath, true);

      inputPath = tmpLocalFilePath;
    } else {
      log.info("Non-HDFS Input path: {}", inputPath);
    }

    String targetPath =
        FsUtils.buildDatasetAttemptPath(
            options, PipelinesVariables.Pipeline.Conversion.FILE_NAME, false);
    String tmpPath = FsUtils.getTempDir(options);

    log.info("Input path: {}", inputPath);
    boolean isDir = Paths.get(inputPath).toFile().isDirectory();

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

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }
}
