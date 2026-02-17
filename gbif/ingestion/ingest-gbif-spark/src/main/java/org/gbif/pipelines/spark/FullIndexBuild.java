package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.*;
import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.EsIndexUtils;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class FullIndexBuild {

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
    FullIndexBuild.Args args = new FullIndexBuild.Args();
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
            args.master, "Rebuild Occurrence Elastic", config, Indexing::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */

    List<String> hdfsPaths = new ArrayList<>();

    FileStatus[] fileStatuses = fileSystem.globStatus(new Path(config.getOutputPath() + "/*"));

    // for each directory, find the last successful interpretation and create a symlink to it
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        String datasetId = fileStatus.getPath().getName();
        FileStatus[] successFiles =
            fileSystem.globStatus(new Path(fileStatus.getPath() + "/*/json/_SUCCESS"));

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

          // create symlink
          //          fileSystem.createSymlink(successAttemptDir, symlinkPath, true);
          System.out.println(
              "Created symlink for dataset " + datasetId + " to " + successAttemptDir.toString());

          // add if the _SUCCESS file is less than 4 weeks old to the list of paths to read from
          if (fileSystem.getFileStatus(successAttemptDir).getModificationTime()
              > System.currentTimeMillis() - 3L * 7 * 24 * 60 * 60 * 1000) {
            hdfsPaths.add(successAttemptDir.toString() + "/hdfs/");
            log.debug(
                "Adding HDFS path for dataset {}: {}", datasetId, successAttemptDir.toString());
          } else {
            log.info(
                "Skipping HDFS path for dataset {}: {} (older than 3 weeks)",
                datasetId,
                successAttemptDir.toString());
          }

        } else {
          // System.out.println("No successful interpretation found for dataset " + datasetId);
        }
      }
    }

    log.info("Starting full index build");

    String coreDwcTerm = "occurrence";

    // load all hdfs view parquet
//    Dataset<Row> hdfs = spark.read().parquet(hdfsPaths.toArray(new String[0])).coalesce(1200);
    Dataset<Row> hdfs = spark.read().parquet(
      new String[]{
        "/Users/djtfmartin/dev/pipelines/gbif/ingestion/ingest-gbif-spark/test-data/02996492-a184-4a55-9735-897a4ab84b18/1/json", // small dataset 1
        "/Users/djtfmartin/dev/pipelines/gbif/ingestion/ingest-gbif-spark/test-data/2669f062-aef9-4677-8669-017897bc0622/1/json", // small dataset 2
        "/Users/djtfmartin/dev/pipelines/gbif/ingestion/ingest-gbif-spark/test-data/27be081b-0250-4cfe-ab5c-e835a66772b8/1/json" //  dataset over 10k
      }
    ).coalesce(1200);

    Dataset<Row> datasetCountsDF = hdfs.groupBy(col("datasetkey")).count().orderBy(desc("count"));

    Map<String, Long> datasetCounts = new HashMap<>();

    for (Row row : datasetCountsDF.collectAsList()) {
      String key = row.getAs("datasetkey");
      Long count = row.getAs("count");
      datasetCounts.put(key, count);
    }

    // create the empty indexes with the schema
    for (Map.Entry<String, Long> entry : datasetCounts.entrySet()) {

      String datasetKey = entry.getKey();
      Long count = entry.getValue();

      String esIndexName =
          count > 10_000L
              ? "full_build_occurrence_" + datasetKey
              : "full_build_occurrence_shared";

      // FIXME - need to be computed...
      Integer indexNumberShards = 5;

      Indexing.ElasticOptions options =
              Indexing.ElasticOptions.fromArgsAndConfig(
                      config,
                      "full_build_occurrence",
                      esIndexName,
                      "elasticsearch/es-occurrence-schema.json",
                      datasetKey, // used for updating the alias
                      1, // used for updating the alias
                      indexNumberShards);

      // Create ES index and alias if not exists
      EsIndexUtils.createIndexAndAliasForDefault(options);
    }

    Dataset<Row> indexingDF =
        hdfs.join(datasetCountsDF, "datasetkey")
            .withColumn(
                "index_name",
                when(
                    col("count").gt(10_000L),
                    concat(
                            lit("full_build_occurrence_"),
                            col("datasetkey")
                    ))
                .otherwise(lit("full_build_occurrence_shared")))
            .drop("count");

    // Write to Elasticsearch
    indexingDF.write()
            .format("org.elasticsearch.spark.sql")
            .option("es.resource", "{index_name}/_doc")
            .mode(SaveMode.Append)
            .option("es.batch.size.entries", config.getElastic().getEsMaxBatchSize())
            .option("es.batch.size.bytes", config.getElastic().getEsMaxBatchSizeBytes())
            .option("es.mapping.id", "gbifId")
            .option("es.nodes.wan.only", "true")
            .option("es.batch.write.refresh", "false")
            .save();
  }
}
