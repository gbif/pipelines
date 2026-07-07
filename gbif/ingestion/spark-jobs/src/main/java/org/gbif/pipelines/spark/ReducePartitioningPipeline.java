package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
@SuppressWarnings("all")
public class ReducePartitioningPipeline {

  @SuppressWarnings({"unused", "FieldMayBeFinal", "WeakerAccess"})
  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = CONFIG_PATH_ARG, description = "Path to YAML configuration file")
    @SuppressWarnings("FieldMayBeFinal")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = SPARK_MASTER_ARG,
        description = "Spark master - there for local dev only",
        required = false)
    @SuppressWarnings("unused")
    private String master;

    @Parameter(
        names = "--datasetKeysFile",
        description =
            "Filename to dump the list of unsuccessful datasets to in HDFS for later review")
    @SuppressWarnings("FieldMayBeFinal")
    private String datasetKeysFile = "/tmp/datasetKeysFile.txt";

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    @SuppressWarnings("unused")
    private boolean help;

    @Parameter(
        names = {"--dryRun", "--dry-run"},
        description = "Dry run: show actions without writing or deleting",
        arity = 1)
    @SuppressWarnings("FieldMayBeFinal")
    private boolean dryRun = false;
  }

  public static void configSparkSession(
      SparkSession.Builder sparkBuilder, PipelinesConfig config) {}

  public static void main(String[] argsv) throws Exception {

    ReducePartitioningPipeline.Args args = new ReducePartitioningPipeline.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    if (config == null) {
      System.err.println(
          "Invalid configuration file. Please provide a valid YAML configuration file.");
      throw new IllegalArgumentException(
          "Invalid configuration file. Missing indexConfig or elastic configuration.");
    }

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master, "Repartitioning", config, ReducePartitioningPipeline::configSparkSession);
    FileSystem fs = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */
    Path path = new Path(args.datasetKeysFile);

    try (FSDataInputStream inputStream = fs.open(path);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

      List<String> datasetUuids =
          reader.lines().map(String::trim).filter(line -> !line.isEmpty()).toList();

      System.out.printf("Found %d dataset UUID(s) to process from %s%n", datasetUuids.size(), path);

      int totalDatasets = datasetUuids.size();
      int processedCount = 0;
      int skippedCount = 0;

      for (int idx = 0; idx < totalDatasets; idx++) {
        String datasetUuid = datasetUuids.get(idx);

        // find each stored attempt
        Path datasetPath = new Path(config.getOutputPath() + "/" + datasetUuid);
        System.out.printf(
            "Processing dataset %d/%d - %s at %s%n",
            idx + 1, totalDatasets, datasetUuid, datasetPath);

        if (!fs.exists(datasetPath)) {
          System.err.printf(
              "Dataset path does not exist, skipping dataset %s: %s%n", datasetUuid, datasetPath);
          skippedCount++;
          continue;
        }
        processedCount++;

        // get attempt subdirectories - listStatus returns immediate children including directories
        FileStatus[] attemptStatuses = fs.listStatus(datasetPath);

        for (FileStatus attemptStatus : attemptStatuses) {
          if (!attemptStatus.isDirectory()) {
            System.out.printf(
                "Skipping non-directory entry under dataset path: %s%n", attemptStatus.getPath());
            continue;
          }
          Path attemptPath = attemptStatus.getPath();
          System.out.printf(
              "Processing stored attempt at %s for dataset %s%n", attemptPath, datasetUuid);

          List<Path> pathsToCoalesce =
              List.of(
                  new Path(attemptPath, "identifiers"),
                  new Path(attemptPath, "identifiers_valid"),
                  new Path(attemptPath, "verbatim"),
                  new Path(attemptPath, "verbatim_ext_filtered"),
                  new Path(attemptPath, "json"),
                  new Path(attemptPath, "hdfs"),
                  new Path(attemptPath, "event_json"),
                  new Path(attemptPath, "event_hdfs"));

          for (Path p : pathsToCoalesce) {
            try {
              System.out.printf("Checking path to coalesce: %s%n", p);
              if (!fs.exists(p)) {
                System.out.printf("Path does not exist: %s%n", p);
                continue;
              }

              FileStatus dirStatus = fs.getFileStatus(p);
              if (!dirStatus.isDirectory()) {
                System.out.printf("Path is not a directory, skipping: %s%n", p);
                continue;
              }

              // Check directory non-empty (ignoring _SUCCESS) and contains _SUCCESS
              FileStatus[] children = fs.listStatus(p);
              boolean hasSuccess = false;
              int dataCount = 0;
              for (FileStatus c : children) {
                String name = c.getPath().getName();
                if ("_SUCCESS".equals(name)) {
                  hasSuccess = true;
                } else {
                  // any other file/dir counts as data
                  dataCount++;
                }
              }

              System.out.printf(
                  "Path %s contains %d data file(s), _SUCCESS present=%b%n",
                  p, dataCount, hasSuccess);

              if (dataCount == 0) {
                System.err.printf("Directory is empty (no parquet files) in: %s - skipping%n", p);
                continue;
              }

              if (!hasSuccess) {
                System.err.printf("_SUCCESS file not found in: %s - skipping%n", p);
                continue;
              }

              String pStr = p.toString();
              String tmpPathStr = pStr + "_coalesced_tmp";
              Path tmpPath = new Path(tmpPathStr);

              System.out.printf("Coalescing parquet at %s -> temporary %s%n", pStr, tmpPathStr);

              if (args.dryRun) {
                System.out.printf(
                    "Dry-run: would read parquet at %s and write coalesced output to %s. No changes will be made.%n",
                    pStr, tmpPathStr);
                continue;
              }

              // Read input dataset and log partition count before coalescing
              Dataset<Row> inputDs = spark.read().parquet(pStr);
              Dataset<Row> ds = inputDs.coalesce(1);

              // write to temporary location
              ds.write().mode(SaveMode.Overwrite).parquet(tmpPathStr);

              // replace original with coalesced output
              boolean deleted = fs.delete(p, true);
              if (!deleted) {
                System.err.printf("Failed to delete original path %s after coalescing%n", p);
                // attempt cleanup of tmp and continue
                fs.delete(tmpPath, true);
                continue;
              }

              System.out.printf(
                  "Deleted original directory %s. Attempting to move coalesced data into place%n",
                  p);

              boolean renamed = fs.rename(tmpPath, p);
              if (!renamed) {
                System.err.printf("Failed to rename tmp path %s to %s%n", tmpPath, p);
                if (fs.exists(tmpPath)) {
                  fs.delete(tmpPath, true);
                }
                continue;
              }

              // Report final file count in the replaced directory
              try {
                FileStatus[] finalChildren = fs.listStatus(p);
                int finalCount = 0;
                for (FileStatus f : finalChildren) {
                  String name = f.getPath().getName();
                  if (!"_SUCCESS".equals(name)) {
                    finalCount++;
                  }
                }
                System.out.printf(
                    "Successfully coalesced parquet files at %s. Final data file count=%d%n",
                    p, finalCount);
              } catch (Exception e) {
                System.err.printf(
                    "Coalesced and moved files into %s but failed to list final contents: %s%n",
                    p, e.getMessage());
              }

            } catch (Exception e) {
              System.err.printf(
                  "Error while trying to coalesce parquet at %s: %s%n", p, e.getMessage());
              e.printStackTrace(System.err);
            }
          }
        }
      }

      System.out.printf(
          "Finished processing datasets. Processed %d of %d datasets; skipped %d (missing paths).%n",
          processedCount, totalDatasets, skippedCount);
    }
  }
}
