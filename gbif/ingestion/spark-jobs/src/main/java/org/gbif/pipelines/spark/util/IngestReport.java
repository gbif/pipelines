package org.gbif.pipelines.spark.util;

import static org.gbif.pipelines.spark.ArgsConstants.CONFIG_PATH_ARG;
import static org.gbif.pipelines.spark.util.FullBuildUtils.getSuccessfulParquetFilePaths;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.Directories;

@Slf4j
public class IngestReport {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = CONFIG_PATH_ARG, description = "Path to YAML configuration file")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  public static void main(String[] argsv) throws IOException {

    IngestReport.Args args = new IngestReport.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);

    Configuration hadoopConf = new Configuration();
    hadoopConf.addResource(new Path(config.getHdfsSiteConfig()));
    hadoopConf.addResource(new Path(config.getCoreSiteConfig()));
    FileSystem fileSystem = FileSystem.get(hadoopConf);

    long runTimestamp = System.currentTimeMillis();

    // occurrence hdfs
    FullBuildUtils.DirectoryScanResult hdfsResult =
        getSuccessfulParquetFilePaths(
            fileSystem,
            config,
            Directories.OCCURRENCE_HDFS,
            config.getRebuildPath()
                + "/unsuccessful_occurrence_hdfs_datasets_"
                + runTimestamp
                + ".txt",
            "");
    log.info("Successful occurrence hdfs datasets: {} ", hdfsResult.successfulPaths().size());

    // occurrence json
    FullBuildUtils.DirectoryScanResult jsonResult =
        getSuccessfulParquetFilePaths(
            fileSystem,
            config,
            Directories.OCCURRENCE_JSON,
            config.getRebuildPath()
                + "/unsuccessful_occurrence_json_datasets_"
                + runTimestamp
                + ".txt",
            "");
    log.info("Successful occurrence json datasets: {} ", jsonResult.successfulPaths().size());

    // occurrence json
    FullBuildUtils.DirectoryScanResult eventHdfs =
        getSuccessfulParquetFilePaths(
            fileSystem,
            config,
            Directories.EVENT_HDFS,
            config.getRebuildPath() + "/unsuccessful_event_hdfs_datasets_" + runTimestamp + ".txt",
            "");
    log.info("Successful event hdfs datasets: {} ", eventHdfs.successfulPaths().size());

    // occurrence json
    FullBuildUtils.DirectoryScanResult eventJson =
        getSuccessfulParquetFilePaths(
            fileSystem,
            config,
            Directories.EVENT_JSON,
            config.getRebuildPath() + "/unsuccessful_event_json_datasets_" + runTimestamp + ".txt",
            "");
    log.info("Successful event json datasets: {} ", eventJson.successfulPaths().size());

    log.info("Scan complete");
  }
}
