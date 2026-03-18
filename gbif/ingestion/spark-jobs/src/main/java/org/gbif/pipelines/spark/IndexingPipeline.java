package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.util.LogUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.spark.util.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.core.config.model.EsConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;
import org.gbif.pipelines.spark.util.EsIndexUtils;

/**
 * Main class for indexing occurrence data to Elasticsearch. It reads Parquet files from HDFS,
 * creates an Elasticsearch index if it doesn't exist, deletes existing records for a specific
 * dataset ID, and writes new records to the index.
 */
@Slf4j
public class IndexingPipeline {

  public static final String METRICS_FILENAME = "occurrence-to-index.yml";

  public static final String ES_INDEX_NAME_ARG = "--esIndexName";
  public static final String ES_INDEX_ALIAS_ARG = "--esIndexAlias";
  public static final String ES_INDEX_NUMBER_OF_SHARDS_ARG = "--indexNumberShards";

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = APP_NAME_ARG, description = "Application name", required = true)
    private String appName;

    @Parameter(names = DATASET_ID_ARG, description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = ATTEMPT_ID_ARG, description = "Attempt number", required = true)
    private int attempt;

    @Parameter(
        names = ES_INDEX_NAME_ARG,
        description = "Name of the Elasticsearch index that will be used to index the records")
    private String esIndexName;

    @Parameter(
        names = ES_INDEX_ALIAS_ARG,
        description = "Alias of the Elasticsearch index that will be used to index the records")
    private String esIndexAlias;

    @Parameter(
        names = ES_INDEX_NUMBER_OF_SHARDS_ARG,
        description = "Number of primary shards in the target index. Default = 3")
    private Integer indexNumberShards = 3;

    @Parameter(names = DATASET_TYPE_ARG, description = "OCCURRENCE or SAMPLING_EVENT")
    private DatasetType datasetType = DatasetType.OCCURRENCE;

    @Parameter(
        names = SOURCE_DIRECTORY_ARG,
        description = "Source directory for parquet files",
        required = true)
    private String sourceDirectory = "json";

    @Parameter(names = CONFIG_PATH_ARG, description = "Path to YAML configuration file")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = SPARK_MASTER_ARG,
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    boolean help;
  }

  public static void main(String[] argsv) throws IOException {

    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(args.master, args.appName, config, IndexingPipeline::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);
    /* ############ standard init block - end ########## */

    if (args.datasetType == DatasetType.OCCURRENCE) {
      runIndexing(
          spark,
          fileSystem,
          config,
          args.datasetId,
          args.attempt,
          args.esIndexAlias,
          args.esIndexName,
          config.getIndexConfig().getOccurrenceSchemaPath(),
          args.indexNumberShards,
          OccurrenceJsonRecord.class,
          args.sourceDirectory);
    } else if (args.datasetType == DatasetType.SAMPLING_EVENT) {
      runIndexing(
          spark,
          fileSystem,
          config,
          args.datasetId,
          args.attempt,
          args.esIndexAlias,
          args.esIndexName,
          config.getIndexConfig().getEventSchemaPath(),
          args.indexNumberShards,
          ParentJsonRecord.class,
          args.sourceDirectory);
    } else {
      log.error("Unsupported dataset type: {}", args.datasetType);
      throw new IllegalArgumentException("Invalid dataset type: " + args.datasetType);
    }

    spark.stop();
    spark.close();
    fileSystem.close();
    System.exit(0);
  }

  public static void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    sparkBuilder.config("es.nodes", String.join(",", config.getElastic().getEsHosts()));
  }

  public static <T> void runIndexing(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      String datasetId,
      Integer attempt,
      String esIndexAlias,
      String esIndexName,
      String esSchemaPath,
      Integer indexNumberShards,
      Class<T> recordClass,
      String parquetDirectoryToLoad) {

    long start = System.currentTimeMillis();
    ThreadContext.put("datasetKey", datasetId);
    ThreadContext.put("attempt", String.valueOf(attempt));
    ThreadContext.put("logger_name", IndexingPipeline.class.getSimpleName());
    ThreadContext.put(
        "step",
        recordClass.equals(OccurrenceJsonRecord.class)
            ? StepType.INTERPRETED_TO_INDEX.name()
            : StepType.EVENTS_INTERPRETED_TO_INDEX.name());

    log.info(
        "Starting index with esIndexName: {}, indexNumberShards: {}",
        esIndexName,
        indexNumberShards);

    // where the pre-prepared json should be
    String inputPath =
        String.format(
            "%s/%s/%d/%s", config.getOutputPath(), datasetId, attempt, parquetDirectoryToLoad);

    // output path for metrics
    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    ElasticOptions options =
        ElasticOptions.fromArgsAndConfig(
            config, esIndexAlias, esIndexName, esSchemaPath, datasetId, attempt, indexNumberShards);

    // Create ES index and alias if not exists
    EsIndexUtils.createIndexAndAliasForDefault(options);

    // Returns indices names in case of swapping
    Set<String> indices = EsIndexUtils.deleteRecordsByDatasetId(options);

    // Read parquet files
    Dataset<T> df = spark.read().parquet(inputPath).as(Encoders.bean(recordClass));

    String esMappingId = recordClass.equals(OccurrenceJsonRecord.class) ? "gbifId" : "internalId";

    // Write to Elasticsearch
    df.write()
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", esIndexName)
        .option("es.batch.size.entries", config.getElastic().getEsMaxBatchSize())
        .option("es.batch.size.bytes", config.getElastic().getEsMaxBatchSizeBytes())
        .option("es.mapping.id", esMappingId)
        .option("es.nodes.wan.only", "true")
        .option("es.batch.write.refresh", "false")
        .mode("append")
        .save();

    EsIndexUtils.updateAlias(options, indices, config.getIndexLock());
    EsIndexUtils.refreshIndex(options);

    long indexCount = df.count();

    // write metrics to yaml
    writeMetricsYaml(
        fileSystem,
        Map.of("parquetToJsonCountAttempted", indexCount),
        outputPath + "/" + METRICS_FILENAME);

    log.info(timeAndRecPerSecond("indexing", start, indexCount));
  }

  @Builder
  @Data
  public static class ElasticOptions {
    String esSchemaPath;
    String esIndexName;
    String[] esAlias;
    String[] esHosts;
    String datasetId;
    Integer attempt;
    @Builder.Default Integer indexNumberShards = 1;
    @Builder.Default String indexRefreshInterval = "40s";
    @Builder.Default Integer indexNumberReplicas = 1;
    @Builder.Default Integer indexMaxResultWindow = 200000;
    @Builder.Default String unassignedNodeDelay = "5m";
    @Builder.Default Boolean useSlowlog = true;
    @Builder.Default String indexSearchSlowlogThresholdQueryWarn = "20s";
    @Builder.Default String indexSearchSlowlogThresholdQueryInfo = "10s";
    @Builder.Default String indexSearchSlowlogThresholdFetchWarn = "2s";
    @Builder.Default String indexSearchSlowlogThresholdFetchInfo = "1s";
    @Builder.Default String indexSearchSlowlogLevel = "info";
    @Builder.Default Integer searchQueryTimeoutSec = 5;
    @Builder.Default Integer searchQueryAttempts = 200;

    public static ElasticOptions fromArgsAndConfig(
        PipelinesConfig config,
        String esIndexAlias,
        String esIndexName,
        String esSchemaPath,
        String datasetId,
        Integer attempt,
        Integer indexNumberShards) {

      EsConfig esConfig = config.getElastic();
      ElasticOptionsBuilder builder =
          ElasticOptions.builder()
              .esIndexName(esIndexName)
              .indexNumberShards(indexNumberShards)
              .indexNumberReplicas(esConfig.getIndexNumberReplicas())
              .esAlias(new String[] {esIndexAlias})
              .datasetId(datasetId)
              .attempt(attempt)
              .esSchemaPath(esSchemaPath)
              .esHosts(esConfig.getEsHosts().split(","));

      if (esConfig.getIndexRefreshInterval() != null) {
        builder.indexRefreshInterval(esConfig.getIndexRefreshInterval());
      }
      if (esConfig.getUnassignedNodeDelay() != null) {
        builder.unassignedNodeDelay(esConfig.getUnassignedNodeDelay());
      }
      if (esConfig.getIndexSearchSlowlogThresholdQueryWarn() != null) {
        builder.indexSearchSlowlogLevel(esConfig.getIndexSearchSlowlogThresholdQueryWarn());
      }
      if (esConfig.getIndexSearchSlowlogThresholdQueryInfo() != null) {
        builder.indexSearchSlowlogThresholdQueryInfo(
            esConfig.getIndexSearchSlowlogThresholdQueryInfo());
      }
      if (esConfig.getIndexSearchSlowlogThresholdFetchWarn() != null) {
        builder.indexSearchSlowlogThresholdFetchWarn(
            esConfig.getIndexSearchSlowlogThresholdFetchWarn());
      }
      if (esConfig.getIndexSearchSlowlogThresholdFetchInfo() != null) {
        builder.indexSearchSlowlogThresholdFetchInfo(
            esConfig.getIndexSearchSlowlogThresholdFetchInfo());
      }
      if (esConfig.getIndexSearchSlowlogLevel() != null) {
        builder.indexSearchSlowlogLevel(esConfig.getIndexSearchSlowlogLevel());
      }
      if (esConfig.getSearchQueryTimeoutSec() != null) {
        builder.searchQueryTimeoutSec(esConfig.getSearchQueryTimeoutSec());
      }
      if (esConfig.getSearchQueryAttempts() != null) {
        builder.searchQueryAttempts(esConfig.getSearchQueryAttempts());
      }
      return builder.build();
    }
  }
}
