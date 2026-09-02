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
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.core.config.model.IndexConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.event.EventHdfsRecord;
import org.gbif.pipelines.io.avro.json.EventJsonRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.spark.util.EsIndexUtils;
import org.gbif.pipelines.spark.util.FullBuildUtils;
import org.gbif.pipelines.spark.util.IndexSettings;
import org.gbif.pipelines.spark.util.PipelineArgs;

/**
 * This class performs a full rebuild of the elastic search index for a given core Darwin Core term
 * (e.g. occurrence or event) by reading from parquet files in HDFS and writing to Elasticsearch. It
 * creates new indexes with the appropriate number of shards based on the record count for each
 * dataset, and updates the alias to point to the new index. It also handles datasets with low
 * record counts by putting them in a shared default index to avoid creating many small indexes in
 * Elasticsearch.
 */
@Slf4j
public class FullIndexBuildPipelineGeoTimeFix {

  @Parameters(separators = "=")
  private static class Args extends PipelineArgs {

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
                + "If false, the new tables will have a prefix and the old tables will not be overwritten.",
        arity = 1)
    private boolean switchOnSuccess = false;

    @Parameter(
        names = "--deleteTempParquetOnSuccess",
        description =
            "Delete the temporary parquet files used for writing to Elastic after a successful build. ",
        arity = 1)
    private boolean deleteTempParquetOnSuccess = true;
  }

  public static void main(String[] argsv) throws Exception {
    Args args = new Args();
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

    // Explicit read schema, since datasets built at different times can have a column typed as
    // float in some parquet files and double in others for fields whose type changed over time
    // (e.g. geologicalContext.range.gt/lte moved from float to double). Forcing those known
    // fields to double up front lets Spark safely up-cast float columns on read, instead of
    // failing when the schema inferred from the first file(s) loaded doesn't match a later
    // file's column type.
    StructType readSchema = recordClassSchema(isOccurrence, args.sourceDirectory);
    for (String[] path : FLOAT_TO_DOUBLE_FIELD_PATHS) {
      readSchema = widenFieldToDouble(readSchema, path, 0);
    }

    // load all hdfs view parquet
    Dataset<Row> hdfs =
        spark
            .read()
            .schema(readSchema)
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

    // sum up the expected size of the default index
    Long defaultIndexPredictedSize = 0L;

    for (Row row : datasetCountsDF.collectAsList()) {
      String key = row.getAs("datasetkey");
      Long count = row.getAs("count");
      datasetCounts.put(key, count);

      // if its a small dataset, it'll be written to the default index
      // track the size of this for shard calc.
      if (count < config.getIndexConfig().getBigIndexIfRecordsMoreThan()) {
        defaultIndexPredictedSize += count;
      }
    }

    long indexCreationTimestamp = Instant.now().toEpochMilli();

    final Map<String, String> datasetToIndexNameMap = new HashMap<>();

    boolean defaultIndexCreated = false;

    String rebuildAlias = esAlias + "_rebuild_" + indexCreationTimestamp;

    // new default name for this rebuild
    final String defaultIndexName =
        createDefaultIndexNamePrefix(config.getIndexConfig(), args.datasetType)
            + "_"
            + indexCreationTimestamp;

    // create the empty indexes with the schema
    for (Map.Entry<String, Long> entry : datasetCounts.entrySet()) {

      String datasetKey = entry.getKey();
      Long recordCount = entry.getValue();
      Integer attempt = scanResult.datasetAttemptMap().get(datasetKey);

      boolean useDefaultIndex =
          recordCount < config.getIndexConfig().getBigIndexIfRecordsMoreThan();

      // avoid trying to create a new index if the record count is low
      // and we already created a default index for another dataset with low record count
      if (recordCount < config.getIndexConfig().getBigIndexIfRecordsMoreThan()
          && defaultIndexCreated) {
        datasetToIndexNameMap.put(datasetKey, defaultIndexName);
        continue;
      }

      String esIndexName =
          useDefaultIndex
              ? defaultIndexName
              : IndexSettings.computeLargeIndexName(
                  args.datasetType,
                  config.getIndexConfig(),
                  datasetKey,
                  attempt,
                  indexCreationTimestamp);

      Integer indexNumberShards =
          getIndexNumberShards(
              useDefaultIndex, config, esIndexName, defaultIndexPredictedSize, recordCount);

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

      if (useDefaultIndex) {
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

    String esMappingId = args.datasetType == DatasetType.OCCURRENCE ? "gbifId" : "internalId";

    // Write to Elasticsearch
    spark
        .read()
        .parquet(config.getRebuildPath() + "/elastic")
        .write()
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", "{index_name}")
        .mode(SaveMode.Append)
        .option("es.batch.size.entries", config.getElastic().getEsMaxBatchSize())
        .option("es.batch.size.bytes", config.getElastic().getEsMaxBatchSizeBytes())
        .option("es.mapping.id", esMappingId)
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

    if (args.deleteTempParquetOnSuccess) {
      fileSystem.delete(new org.apache.hadoop.fs.Path(config.getRebuildPath() + "/elastic"), true);
    }

    fileSystem.close();
    spark.stop();
    spark.close();

    if (args.switchOnSuccess) {
      EsIndexUtils.swapIndices(rebuildAlias, esAlias, hosts);
    }

    log.info("Full index build completed");
  }

  // Fields whose type has changed from float to double over time (e.g. geologicalContext.range
  // was float, see common-json-record.avsc history), so older and newer parquet files can
  // disagree on the column type. Add a path here if another field is found to have drifted.
  private static final String[][] FLOAT_TO_DOUBLE_FIELD_PATHS = {
    {"geologicalContext", "range", "gt"}, {"geologicalContext", "range", "lte"}
  };

  private static StructType recordClassSchema(boolean isOccurrence, String sourceDirectory) {
    Class<?> recordClass;
    if (isOccurrence) {
      recordClass =
          Directories.OCCURRENCE_HDFS.equals(sourceDirectory)
              ? OccurrenceHdfsRecord.class
              : OccurrenceJsonRecord.class;
    } else {
      recordClass =
          Directories.EVENT_HDFS.equals(sourceDirectory)
              ? EventHdfsRecord.class
              : EventJsonRecord.class;
    }
    return Encoders.bean(recordClass).schema();
  }

  /**
   * Sets the type of the field at {@code path} (e.g. {@code {"geologicalContext", "range", "gt"}})
   * to {@link DataTypes#DoubleType}. A no-op if the path isn't present in {@code schema} (e.g. the
   * record type doesn't have that field), so it's safe to apply the same path to schemas that don't
   * contain it.
   */
  private static StructType widenFieldToDouble(StructType schema, String[] path, int depth) {
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      if (!field.name().equals(path[depth])) {
        continue;
      }
      StructField[] newFields = fields.clone();
      if (depth == path.length - 1) {
        newFields[i] =
            new StructField(field.name(), DataTypes.DoubleType, field.nullable(), field.metadata());
      } else if (field.dataType() instanceof StructType) {
        StructType nested = widenFieldToDouble((StructType) field.dataType(), path, depth + 1);
        newFields[i] = new StructField(field.name(), nested, field.nullable(), field.metadata());
      } else {
        return schema;
      }
      return new StructType(newFields);
    }
    return schema;
  }

  private static Integer getIndexNumberShards(
      boolean useDefaultIndex,
      PipelinesConfig config,
      String esIndexName,
      Long defaultIndexPredictedSize,
      Long recordCount) {

    IndexConfig indexConfig = config.getIndexConfig();

    if (useDefaultIndex) {
      int computedShards =
          IndexSettings.computeNumberOfShards(indexConfig, esIndexName, defaultIndexPredictedSize);

      int toUse = Math.max(indexConfig.defaultIndexMinShards, computedShards);
      log.info(
          "Computed shards for default index {}, min {}, will use {}",
          computedShards,
          indexConfig.defaultIndexMinShards,
          toUse);
      return toUse;
    }

    return IndexSettings.computeNumberOfShards(indexConfig, esIndexName, recordCount.intValue());
  }
}
