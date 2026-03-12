package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.*;
import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.util.EsIndexUtils.createDefaultIndexNamePrefix;
import static org.gbif.pipelines.spark.util.FullBuildUtils.checkDatasetTypeSupported;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.time.Instant;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.spark.util.EsIndexUtils;
import org.gbif.pipelines.spark.util.FullBuildUtils;
import org.gbif.pipelines.spark.util.IndexSettings;

/**
 * This class performs a full rebuild of the elastic search index for a given core Darwin Core term
 * (e.g. occurrence or event) by reading from parquet files in HDFS and writing to Elasticsearch. It
 * creates new indexes with the appropriate number of shards based on the record count for each
 * dataset, and updates the alias to point to the new index. It also handles datasets with low
 * record counts by putting them in a shared default index to avoid creating many small indexes in
 * Elasticsearch.
 */
@Slf4j
public class FullIndexBuildPipeline {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(
        names = CONFIG_PATH_ARG,
        description = "Path to YAML configuration file",
        required = true)
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = SPARK_MASTER_ARG,
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(names = NUMBER_OF_SHARDS_ARG, description = "Number of shards")
    private int numberOfShards = 2400;

    @Parameter(
        names = "--maxRecordsPerFile",
        description = "Max records per file when writing to HDFS before writing to Elastic")
    private int maxRecordsPerFile = 200_000;

    @Parameter(
        names = SOURCE_DIRECTORY_ARG,
        description = "Directory containing the parquet to load",
        required = true)
    private String sourceDirectory = "json";

    @Parameter(names = DATASET_TYPE_ARG, description = "OCCURRENCE or SAMPLING_EVENT")
    private DatasetType datasetType = DatasetType.OCCURRENCE;

    @Parameter(
        names = UNSUCCESSFUL_DUMP_FILENAME,
        description =
            "Filename to dump the list of unsuccessful datasets to in HDFS for later review")
    private String unsuccessfulDumpFilename = "unsuccessful-elastic-datasets.txt";

    @Parameter(
        names = "--earliestModificationTime",
        description =
            "Only consider parquet files modified after this time (ISO 8601 format, e.g. 2024-01-01T00:00:00Z)")
    private String earliestModificationTime = null;

    @Parameter(
        names = SWITCH_ON_SUCCESS,
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
    FullIndexBuildPipeline.Args args = new FullIndexBuildPipeline.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    if (config == null || config.getIndexConfig() == null || config.getElastic() == null) {
      log.error("Invalid configuration file. Please provide a valid YAML configuration file.");
      throw new IllegalArgumentException(
          "Invalid configuration file. Missing indexConfig or elastic configuration.");
    }

    checkDatasetTypeSupported(args.datasetType);

    boolean isOccurrence = args.datasetType == DatasetType.OCCURRENCE;

    final String esAlias =
        isOccurrence
            ? config.getIndexConfig().getOccurrenceAlias()
            : config.getIndexConfig().getEventAlias();

    final String schemaPath =
        isOccurrence
            ? config.getIndexConfig().getOccurrenceSchemaPath()
            : config.getIndexConfig().getEventSchemaPath();

    final String versionPath =
        isOccurrence
            ? config.getIndexConfig().getOccurrenceVersion()
            : config.getIndexConfig().getEventVersion();

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master,
            "Rebuild Occurrence Elastic",
            config,
            IndexingPipeline::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */

    FullBuildUtils.DirectoryScanResult scanResult =
        FullBuildUtils.getSuccessfulParquetFilePaths(
            fileSystem,
            config,
            args.sourceDirectory,
            config.getRebuildPath() + "/" + args.unsuccessfulDumpFilename,
            args.earliestModificationTime);

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

    long indexCreationTimestamp = Instant.now().toEpochMilli();

    final Map<String, String> datasetToIndexNameMap = new HashMap<>();

    boolean defaultIndexCreated = false;

    String rebuildAlias = esAlias + "_rebuild_" + indexCreationTimestamp;

    // new default name for this rebuild
    final String defaultIndexName =
        createDefaultIndexNamePrefix(config, args.datasetType) + "_" + indexCreationTimestamp;

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

      String esIndexName =
          recordCount < config.getIndexConfig().getBigIndexIfRecordsMoreThan()
              ? defaultIndexName
              : IndexSettings.computeLargeIndexName(
                  args.datasetType,
                  config.getIndexConfig(),
                  datasetKey,
                  attempt,
                  indexCreationTimestamp);

      Integer indexNumberShards =
          IndexSettings.computeNumberOfShards(
              config.getIndexConfig(), esIndexName, recordCount.intValue());

      IndexingPipeline.ElasticOptions options =
          IndexingPipeline.ElasticOptions.fromArgsAndConfig(
              config,
              rebuildAlias,
              esIndexName,
              schemaPath,
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
                    col("count").geq(config.getIndexConfig().getBigIndexIfRecordsMoreThan()),
                    concat(
                        col("datasetkey"),
                        lit("_"),
                        col("attempt"),
                        lit("_"),
                        lit(versionPath),
                        lit("_"),
                        lit(indexCreationTimestamp)))
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

    // get es hosts
    String[] hosts =
        Arrays.stream(config.getElastic().getEsHosts().split(","))
            .map(String::trim)
            .toArray(String[]::new);
    EsConfig esConfig = EsConfig.from(hosts);

    try (EsClient esClient = EsClient.from(esConfig)) {
      Set<String> uniqueIndexNames = new HashSet<>(datasetToIndexNameMap.values());
      for (String indexName : uniqueIndexNames) {
        log.info("Refreshing index {}", indexName);
        EsService.refreshIndex(esClient, indexName);
      }
    }
    fileSystem.close();
    spark.stop();
    spark.close();

    if (args.switchOnSuccess) {
      EsIndexUtils.swapIndices(rebuildAlias, esAlias, hosts);
    }
    log.info("Full index build completed");
  }
}
