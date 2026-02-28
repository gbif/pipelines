package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.*;
import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.FullTableBuild.SUPPORTED_CORE_TERMS;
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

/**
 * This class performs a full rebuild of the elastic search index for a given core Darwin Core term
 * (e.g. occurrence or event) by reading from parquet files in HDFS and writing to Elasticsearch. It
 * creates new indexes with the appropriate number of shards based on the record count for each
 * dataset, and updates the alias to point to the new index. It also handles datasets with low
 * record counts by putting them in a shared default index to avoid creating many small indexes in
 * Elasticsearch.
 */
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
        names = "--coreDwcTerm",
        description = "Core Darwin Core term to build the table for, e.g. 'occurrence' or 'event'",
        required = true)
    private String coreDwcTerm = "occurrence";

    @Parameter(
        names = "--unsuccessfulDumpFilename",
        description =
            "Filename to dump the list of unsuccessful datasets to in HDFS for later review")
    private String unsuccessfulDumpFilename = "unsuccessful-elastic-datasets.txt";

    @Parameter(
        names = "--earliestModificationTime",
        description =
            "Only consider parquet files modified after this time (ISO 8601 format, e.g. 2024-01-01T00:00:00Z)")
    private String earliestModificationTime = null;

    @Parameter(
        names = "--switchOnSuccess",
        description =
            "Switch the new tables to the final names (e.g. 'occurrence' or 'event') after successful build. "
                + "If false, the new tables will have a prefix and the old tables will not be overwritten.")
    private boolean switchOnSuccess = false;

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

    if (args.coreDwcTerm == null
        || args.coreDwcTerm.isEmpty()
        || !SUPPORTED_CORE_TERMS.contains(args.coreDwcTerm)) {
      log.error("coreDwcTerm is required and cannot be empty");
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

    //    IngestUtils.DirectoryScanResult scanResult =
    //        IngestUtils.getSuccessFulParquetFilePaths(
    //            fileSystem,
    //            config,
    //            args.sourceDirectory,
    //            config.getRebuildPath() + "/" + args.unsuccessfulDumpFilename,
    //            args.earliestModificationTime);

    // For testing - hardcode the scan result to avoid hitting HDFS and speed up development.
    // The paths should be to the directories containing the parquet files, not the parquet files
    // themselves.
    IngestUtils.DirectoryScanResult scanResult =
        new IngestUtils.DirectoryScanResult(
            List.of(
                "/Users/djtfmartin/dev/pipelines/gbif/ingestion/ingest-gbif-spark/test-data/02996492-a184-4a55-9735-897a4ab84b18/1/json/",
                "/Users/djtfmartin/dev/pipelines/gbif/ingestion/ingest-gbif-spark/test-data/23b714b1-fae6-4074-bdd0-5e17ae3b5f11/1/json/",
                "/Users/djtfmartin/dev/pipelines/gbif/ingestion/ingest-gbif-spark/test-data/2669f062-aef9-4677-8669-017897bc0622/1/json/",
                "/Users/djtfmartin/dev/pipelines/gbif/ingestion/ingest-gbif-spark/test-data/27be081b-0250-4cfe-ab5c-e835a66772b8/1/json/"),
            Map.of(
                "02996492-a184-4a55-9735-897a4ab84b18",
                1,
                "23b714b1-fae6-4074-bdd0-5e17ae3b5f11",
                1,
                "2669f062-aef9-4677-8669-017897bc0622",
                1,
                "27be081b-0250-4cfe-ab5c-e835a66772b8",
                1));

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

    String rebuildAlias =
        config.getIndexConfig().getOccurrenceAlias() + "_rebuild_" + System.currentTimeMillis();

    // new default name for this rebuild
    final String defaultIndexName =
        config.getIndexConfig().getDefaultPrefixName()
            + "_"
            + config.getIndexConfig().getOccurrenceAlias()
            + "_"
            + timestamp;

    // create the empty indexes with the schema
    for (Map.Entry<String, Long> entry : datasetCounts.entrySet()) {

      String datasetKey = entry.getKey();
      Long recordCount = entry.getValue();
      Integer attempt = scanResult.datasetAttemptMap().get(datasetKey);

      // avoid trying to create a new index if the record count is low
      // and we already created a default index for another dataset with low record count
      if (recordCount < config.getIndexConfig().getBigIndexIfRecordsMoreThan()
          && defaultIndexCreated) {
        datasetToIndexNameMap.put(datasetKey, defaultIndexName);
        continue;
      }

      DatasetType datasetType =
          args.coreDwcTerm.equalsIgnoreCase("occurrence")
              ? DatasetType.OCCURRENCE
              : DatasetType.SAMPLING_EVENT;

      // FIXME
      String esIndexName =
          recordCount < config.getIndexConfig().getBigIndexIfRecordsMoreThan()
              ? defaultIndexName
              : IndexSettings.computeIndexName(
                  datasetType,
                  config.getIndexConfig(),
                  httpClient,
                  datasetKey,
                  attempt,
                  recordCount.intValue(),
                  timestamp);

      Integer indexNumberShards =
          IndexSettings.computeNumberOfShards(
              config.getIndexConfig(), esIndexName, recordCount.intValue());

      Indexing.ElasticOptions options =
          Indexing.ElasticOptions.fromArgsAndConfig(
              config,
              rebuildAlias,
              esIndexName,
              Indexing.ELASTICSEARCH_ES_OCCURRENCE_SCHEMA_JSON, // FIXME
              datasetKey, // used for updating the alias
              attempt,
              indexNumberShards);

      // Create ES index and alias if not exists
      EsIndexUtils.createIndexAndAliasForDefault(options);
      datasetToIndexNameMap.put(datasetKey, esIndexName);

      if (recordCount < config.getIndexConfig().getBigIndexIfRecordsMoreThan()) {
        defaultIndexCreated = true;
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
        .parquet(config.getRebuildPath() + "/elastic");

    // Write to Elasticsearch
    spark
        .read()
        .parquet(config.getRebuildPath() + "/elastic")
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

    if (args.switchOnSuccess) {
      EsIndexUtils.swapIndicies(
          rebuildAlias,
          config.getIndexConfig().getOccurrenceAlias(),
          config.getElastic().getEsHosts());
    }
    log.info("Full index build completed");
  }
}
