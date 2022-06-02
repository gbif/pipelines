package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.DwcaToVerbatimPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
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
          FileSystemFactory.getInstance(
                  HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()))
              .getFs(options.getInputPath());

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
        String.join(
            "/",
            options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "verbatim");

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
