package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.*;
import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.time.Instant;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
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
import org.gbif.pipelines.IngestUtils;
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

    @Parameter(names = "--numberOfShards", description = "Number of shards")
    private int numberOfShards = 2400;

    @Parameter(
        names = "--maxRecordsPerFile",
        description = "Max records per file when writing to HDFS before writing to Elastic")
    private int maxRecordsPerFile = 200_000;

    @Parameter(
        names = "--sourceDirectory",
        description = "Directory containing the parquet to load",
        required = true)
    private String sourceDirectory = "json";

    @Parameter(
        names = "--unsuccessfulDumpFilename",
        description =
            "Filename to dump the list of unsuccessful datasets to in HDFS for later review",
        required = true)
    private String unsuccessfulDumpFilename = "unsuccessful-elastic-datasets.txt";

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

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master, "Rebuild Occurrence Elastic", config, Indexing::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */

    IngestUtils.DirectoryScanResult scanResult =
        IngestUtils.getSuccessFulParquetFilePaths(
            fileSystem,
            config,
            args.sourceDirectory,
            config.getRebuildPath() + "/" + args.unsuccessfulDumpFilename);

    log.info("Starting full index build");

    if (scanResult.successfulPaths().isEmpty()) {
      log.warn("No datasets with successful interpretations found. Exiting.");
      return;
    }

    // load all hdfs view parquet
    Dataset<Row> hdfs =
        spark
            .read()
            .parquet(scanResult.successfulPaths().toArray(new String[0]))
            .coalesce(args.numberOfShards);

    spark
        .udf()
        .register(
            "getAttemptUDF",
            (String datasetKey) -> scanResult.datasetAttemptMap().get(datasetKey),
            DataTypes.IntegerType);

    Dataset<Row> datasetCountsDF =
        hdfs.groupBy(col("datasetkey"))
            .count()
            .orderBy(desc("count"))
            .withColumn("attempt", callUDF("getAttemptUDF", col("datasetkey")));

    datasetCountsDF.show(10000, false);

    final Map<String, Long> datasetCounts = new HashMap<>();

    for (Row row : datasetCountsDF.collectAsList()) {
      String key = row.getAs("datasetkey");
      Long count = row.getAs("count");
      datasetCounts.put(key, count);
    }

    long timestamp = Instant.now().toEpochMilli();

    CloseableHttpClient httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();

    final Map<String, String> datasetToIndexNameMap = new HashMap<>();

    boolean defaultIndexCreated = false;
    String defaultIndexName = null;

    // create the empty indexes with the schema
    for (Map.Entry<String, Long> entry : datasetCounts.entrySet()) {

      String datasetKey = entry.getKey();
      Long recordCount = entry.getValue();

      // avoid trying to create a new index if the record count is low
      // and we already created a default index for another dataset with low record count
      if (recordCount < config.getIndexConfig().getBigIndexIfRecordsMoreThan()
          && defaultIndexCreated) {
        datasetToIndexNameMap.put(datasetKey, defaultIndexName);
        continue;
      }

      String esIndexName =
          IndexSettings.computeIndexName(
              DatasetType.OCCURRENCE,
              config.getIndexConfig(),
              httpClient,
              datasetKey,
              scanResult.datasetAttemptMap().get(datasetKey),
              recordCount.intValue(),
              timestamp);

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
              scanResult.datasetAttemptMap().get(datasetKey),
              indexNumberShards);

      // Create ES index and alias if not exists
      EsIndexUtils.createIndexAndAliasForDefault(options);
      datasetToIndexNameMap.put(datasetKey, esIndexName);

      if (recordCount < config.getIndexConfig().getBigIndexIfRecordsMoreThan()) {
        defaultIndexCreated = true;
        defaultIndexName =
            IndexSettings.getDefaultSharedIndexName(
                config.getIndexConfig(),
                config.getIndexConfig().getOccurrenceVersion(),
                httpClient);
      } else {
        EsConfig esConfig = EsConfig.from(options.getEsHosts());
        try (EsClient esClient = EsClient.from(esConfig)) {
          EsIndexUtils.addIndexAlias(esClient, options, esIndexName);
        }
      }
    }

    // datasetId + "_" + attempt + "_" + indexVersion + "_" + timestamp;
    hdfs.join(broadcast(datasetCountsDF), "datasetkey")
        .withColumn(
            "index_name",
            when(
                    col("count").gt(config.getIndexConfig().getBigIndexIfRecordsMoreThan()),
                    concat(
                        col("datasetkey"),
                        lit("_"),
                        col("attempt"),
                        lit("_"),
                        lit(config.getIndexConfig().getOccurrenceVersion()),
                        lit("_"),
                        lit(timestamp)))
                .otherwise(lit(defaultIndexName)))
        .write()
        .option("maxRecordsPerFile", args.maxRecordsPerFile)
        .mode(SaveMode.Overwrite)
        .parquet("hdfs://gbif-hdfs/data/rebuild_lab/elastic");

    // Write to Elasticsearch
    spark
        .read()
        .parquet("hdfs://gbif-hdfs/data/rebuild_lab/elastic")
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
      datasetToIndexNameMap.values().stream()
          .distinct()
          .forEach(
              indexName -> {
                log.info("Refreshing index " + indexName);
                EsService.refreshIndex(esClient, indexName);
              });
    }

    fileSystem.close();
    spark.stop();
    spark.close();
    log.info("Full index build completed");
  }
}
