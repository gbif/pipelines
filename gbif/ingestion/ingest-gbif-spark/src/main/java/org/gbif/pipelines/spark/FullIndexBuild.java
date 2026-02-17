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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.EsIndexUtils;
import org.gbif.pipelines.IndexSettings;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.estools.service.EsService;

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

    PipelinesConfig config = loadConfig(args.config);
    assert config != null && config.getIndexConfig() != null && config.getElastic() != null;

    final Map<String, Integer> datasetAttemptMap = new HashMap<>();

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

          String attempt = successAttemptDir.getName(); // this should be the attempt number
          System.out.println(
              "Created symlink for dataset " + datasetId + " to " + successAttemptDir.toString());

          // add if the _SUCCESS file is less than 4 weeks old to the list of paths to read from
          hdfsPaths.add(successAttemptDir.toString() + "/json/");
          datasetAttemptMap.put(datasetId, Integer.parseInt(attempt));
          log.debug("Adding HDFS path for dataset {}: {}", datasetId, successAttemptDir.toString());

        } else {
          // System.out.println("No successful interpretation found for dataset " + datasetId);
        }
      }
    }

    log.info("Starting full index build");

    if (hdfsPaths.isEmpty()) {
      log.warn("No datasets with successful interpretations found. Exiting.");
      return;
    }

    // load all hdfs view parquet
    Dataset<Row> hdfs = spark.read().parquet(hdfsPaths.toArray(new String[0])).coalesce(1200);
    Dataset<Row> datasetCountsDF = hdfs.groupBy(col("datasetkey")).count().orderBy(desc("count"));

    final Map<String, Long> datasetCounts = new HashMap<>();

    for (Row row : datasetCountsDF.collectAsList()) {
      String key = row.getAs("datasetkey");
      Long count = row.getAs("count");
      datasetCounts.put(key, count);
    }

    CloseableHttpClient httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();

    final Map<String, String> datasetToIndexNameMap = new HashMap<>();
    // create the empty indexes with the schema
    for (Map.Entry<String, Long> entry : datasetCounts.entrySet()) {

      String datasetKey = entry.getKey();
      Long recordCount = entry.getValue();

      String esIndexName =
          IndexSettings.computeIndexName(
              DatasetType.OCCURRENCE,
              config.getIndexConfig(),
              httpClient,
              datasetKey,
              datasetAttemptMap.get(datasetKey),
              recordCount.intValue());

      Integer indexNumberShards =
          IndexSettings.computeNumberOfShards(
              config.getIndexConfig(), esIndexName, recordCount.intValue());

      Indexing.ElasticOptions options =
          Indexing.ElasticOptions.fromArgsAndConfig(
              config,
              config.getIndexConfig().getOccurrenceAlias(),
              esIndexName,
              "elasticsearch/es-occurrence-schema.json",
              datasetKey, // used for updating the alias
              datasetAttemptMap.get(datasetKey),
              indexNumberShards);

      // Create ES index and alias if not exists
      EsIndexUtils.createIndexAndAliasForDefault(options);

      datasetToIndexNameMap.put(datasetKey, esIndexName);
    }

    spark
        .udf()
        .register(
            "computeIndexNameUDF",
            (String datasetKey) -> datasetToIndexNameMap.get(datasetKey),
            DataTypes.StringType);

    Dataset<Row> indexingDF =
        hdfs.join(datasetCountsDF, "datasetkey")
            .withColumn("index_name", callUDF("computeIndexNameUDF", col("datasetkey")));

    // Write to Elasticsearch
    indexingDF
        .write()
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", "{index_name}/_doc")
        .mode(SaveMode.Append)
        .option("es.batch.size.entries", config.getElastic().getEsMaxBatchSize())
        .option("es.batch.size.bytes", config.getElastic().getEsMaxBatchSizeBytes())
        .option("es.mapping.id", "gbifId")
        .option("es.nodes.wan.only", "true")
        .option("es.batch.write.refresh", "false")
        .save();

    try (EsClient esClient = EsClient.from(EsConfig.from(config.getElastic().getEsHosts()))) {
      datasetToIndexNameMap
          .values()
          .forEach(
              indexName -> {
                System.out.println("Refreshing index " + indexName);
                EsService.refreshIndex(esClient, indexName);
              });
    }
    System.out.println("Full index build completed");
  }
}
