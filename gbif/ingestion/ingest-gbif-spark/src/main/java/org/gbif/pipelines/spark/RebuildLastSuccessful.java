package org.gbif.pipelines.spark;

import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

public class RebuildLastSuccessful {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(
        names = "--config",
        description = "Path to YAML configuration file",
        required = false)
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = "--master",
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  public static void main(String[] argsv) throws Exception {
    RebuildLastSuccessful.Args args = new RebuildLastSuccessful.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    long start = System.currentTimeMillis();

    PipelinesConfig config = loadConfig(args.config);

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master, "Rebuild symlinks", config, OccurrenceInterpretation::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */

    FileStatus[] fileStatuses = fileSystem.globStatus(new Path(config.getOutputPath() + "/*"));

    // for each directory, find the last successful interpretation and create a symlink to it
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        String datasetId = fileStatus.getPath().getName();
        FileStatus[] successFiles =
            fileSystem.globStatus(new Path(fileStatus.getPath() + "/*/hdfs/_SUCCESS"));

        // find the newest _SUCCESS file
        FileStatus newestSuccessFile = null;
        for (FileStatus successFile : successFiles) {
          if (newestSuccessFile == null
              || successFile.getModificationTime() > newestSuccessFile.getModificationTime()) {
            newestSuccessFile = successFile;
          }
        }
        if (newestSuccessFile != null) {
          Path successAttemptDir =
              newestSuccessFile
                  .getPath()
                  .getParent()
                  .getParent(); // go up from hdfs/_SUCCESS to interpretation dir
          Path symlinkPath = new Path(fileStatus.getPath(), "last-successful");

          // delete existing symlink if it exists
          if (fileSystem.exists(symlinkPath)) {
            fileSystem.delete(symlinkPath, false);
          }

          // add if the _SUCCESS file is less than 4 weeks old to the list of paths to read from
          if (fileSystem.getFileStatus(successAttemptDir).getModificationTime()
              > System.currentTimeMillis() - 3L * 7 * 24 * 60 * 60 * 1000) {
            //            System.out.println(
            //                String.format(
            //                    "Adding HDFS path for dataset %s",
            //                    datasetId,
            //                    successAttemptDir.toString()));
          } else {
            System.out.printf("Older than 3 weeks %s%n", datasetId);
          }
        } else {
          System.out.println("No successful interpretation found for dataset " + datasetId);
        }
      }
    }

    fileSystem.close();
    spark.stop();
    spark.close();

    long end = System.currentTimeMillis();
    // print time taken in seconds
    System.out.println("Time taken: " + (end - start) / 1000 + " seconds");
  }
}
