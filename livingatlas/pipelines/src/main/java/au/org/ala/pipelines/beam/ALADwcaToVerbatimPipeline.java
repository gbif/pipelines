package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.DwcaToVerbatimPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ArchiveUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.DwcaExtendedRecordIO;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.slf4j.MDC;

/** Wrapper around DwcaToVerbatimPipeline to allow for Yaml config setup. */
@Slf4j
public class ALADwcaToVerbatimPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "dwca-avro");
    DwcaToVerbatimPipelineOptions options =
        PipelinesOptionsFactory.create(DwcaToVerbatimPipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.VERBATIM_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(DwcaToVerbatimPipelineOptions options) throws Exception {

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

    // save the JSON descriptor to file
    Map<String, Object> archiveProperties = ArchiveUtils.getArchiveProperties(inputPath);
    String targetPath =
        String.join(
            "/",
            options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "verbatim",
            "verbatim");

    String tmpPath = PathBuilder.getTempDir(options);

    log.info("Input path: {}", inputPath);
    boolean isDir = Paths.get(inputPath).toFile().isDirectory();

    log.info("Input path is isDir {}", isDir);
    DwcaExtendedRecordIO.Read reader =
        isDir
            ? DwcaExtendedRecordIO.Read.fromLocation(inputPath)
            : DwcaExtendedRecordIO.Read.fromCompressed(inputPath, tmpPath);

    log.info("Adding step 2: Pipeline steps");
    Pipeline p = Pipeline.create(options);

    log.info("Write path: {}", targetPath);
    p.apply("Read from Darwin Core Archive", reader)
        .apply("Write to avro", VerbatimTransform.create().write(targetPath));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    // write metrics
    saveCountersToTargetPathFile(options, result.metrics(), archiveProperties);

    log.info("Pipeline has been finished");
  }

  public static void saveCountersToTargetPathFile(
      InterpretationPipelineOptions options,
      MetricResults results,
      Map<String, Object> archiveProperties)
      throws Exception {

    // create a map with metrics nd properties
    HashMap<String, Object> properties = new HashMap<>();

    MetricQueryResults queryResults = results.queryMetrics(MetricsFilter.builder().build());
    queryResults
        .getCounters()
        .forEach(x -> properties.put(x.getName().getName() + "Attempted", x.getAttempted()));
    properties.putAll(archiveProperties);

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    // create YAML
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    String path = PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, path);
    FsUtils.createFile(fs, path, mapper.writeValueAsString(properties));
  }
}
