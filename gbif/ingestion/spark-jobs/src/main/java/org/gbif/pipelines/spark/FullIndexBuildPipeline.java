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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
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
public class FullIndexBuildPipeline {

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

    // Datasets built at different times can have a column typed as float in some parquet files
    // and double in others for fields whose type changed over time (e.g.
    // geologicalContext.range.gt/lte moved from float to double). Spark's Parquet reader can't
    // implicitly convert between float and double in either direction (it throws
    // SchemaColumnConvertNotSupportedException), so a single explicit schema can't reconcile a
    // mix of both on its own. Instead: split the paths by what's actually on disk for the
    // known-drifted field(s), read each group with a schema matching its physical type, cast the
    // float group up to double after decoding (a normal, always-safe Catalyst cast), then union.
    Dataset<Row> hdfs =
        readWithDriftedFieldsAsDouble(
                spark, scanResult.successfulPaths(), isOccurrence, args.sourceDirectory)
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

  private static final int SCHEMA_PROBE_PARALLELISM = 32;

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
   * Reads {@code paths}, guaranteeing that every field in {@link #FLOAT_TO_DOUBLE_FIELD_PATHS}
   * comes back as a double column, regardless of whether it's stored as float or double on disk in
   * any given file.
   */
  private static Dataset<Row> readWithDriftedFieldsAsDouble(
      SparkSession spark, List<String> paths, boolean isOccurrence, String sourceDirectory) {

    StructType baseSchema = recordClassSchema(isOccurrence, sourceDirectory);

    // Paths are bucketed by physical type of the first configured field; the paths in
    // FLOAT_TO_DOUBLE_FIELD_PATHS are assumed to always drift together (they were introduced and
    // changed in lockstep), so one representative field is enough to classify a file.
    Map<Boolean, List<String>> partitioned =
        partitionPathsByFieldType(spark, paths, FLOAT_TO_DOUBLE_FIELD_PATHS[0]);
    List<String> legacyFloatPaths = partitioned.get(true);
    List<String> currentDoublePaths = partitioned.get(false);

    Dataset<Row> currentDf = null;
    if (!currentDoublePaths.isEmpty()) {
      StructType doubleSchema = baseSchema;
      for (String[] path : FLOAT_TO_DOUBLE_FIELD_PATHS) {
        doubleSchema = setFieldType(doubleSchema, path, 0, DataTypes.DoubleType);
      }
      currentDf =
          spark.read().schema(doubleSchema).parquet(currentDoublePaths.toArray(new String[0]));
    }

    Dataset<Row> legacyDf = null;
    if (!legacyFloatPaths.isEmpty()) {
      StructType floatSchema = baseSchema;
      for (String[] path : FLOAT_TO_DOUBLE_FIELD_PATHS) {
        floatSchema = setFieldType(floatSchema, path, 0, DataTypes.FloatType);
      }
      legacyDf = spark.read().schema(floatSchema).parquet(legacyFloatPaths.toArray(new String[0]));
      for (String[] path : FLOAT_TO_DOUBLE_FIELD_PATHS) {
        legacyDf = castFieldToDouble(legacyDf, path);
      }
    }

    if (currentDf != null && legacyDf != null) {
      return currentDf.unionByName(legacyDf);
    }
    return currentDf != null ? currentDf : legacyDf;
  }

  /**
   * Probes each path's actual on-disk schema (metadata only, no data scan) to determine whether
   * {@code fieldPath} is stored as float there, and buckets the paths accordingly. Probing is
   * parallelized on the driver since it's pure I/O (footer reads over many small HDFS directories),
   * not a Spark job.
   */
  private static Map<Boolean, List<String>> partitionPathsByFieldType(
      SparkSession spark, List<String> paths, String[] fieldPath) {

    ExecutorService executor = Executors.newFixedThreadPool(SCHEMA_PROBE_PARALLELISM);
    try {
      List<Future<Boolean>> isFloatByPath =
          paths.stream()
              .map(path -> executor.submit(() -> isFieldFloat(spark, path, fieldPath)))
              .collect(Collectors.toList());

      Map<Boolean, List<String>> result = new HashMap<>();
      result.put(true, new ArrayList<>());
      result.put(false, new ArrayList<>());
      for (int i = 0; i < paths.size(); i++) {
        try {
          result.get(isFloatByPath.get(i).get()).add(paths.get(i));
        } catch (Exception e) {
          throw new RuntimeException("Failed to probe schema for path " + paths.get(i), e);
        }
      }
      return result;
    } finally {
      executor.shutdown();
    }
  }

  private static boolean isFieldFloat(SparkSession spark, String path, String[] fieldPath) {
    StructType schema = spark.read().parquet(path).schema();
    DataType type = findFieldType(schema, fieldPath, 0);
    return DataTypes.FloatType.equals(type);
  }

  private static DataType findFieldType(StructType schema, String[] path, int depth) {
    for (StructField field : schema.fields()) {
      if (!field.name().equals(path[depth])) {
        continue;
      }
      if (depth == path.length - 1) {
        return field.dataType();
      }
      return field.dataType() instanceof StructType
          ? findFieldType((StructType) field.dataType(), path, depth + 1)
          : null;
    }
    return null;
  }

  /**
   * Sets the type of the field at {@code path} (e.g. {@code {"geologicalContext", "range", "gt"}})
   * to {@code targetType}. A no-op if the path isn't present in {@code schema} (e.g. the record
   * type doesn't have that field), so it's safe to apply the same path to schemas that don't
   * contain it.
   */
  private static StructType setFieldType(
      StructType schema, String[] path, int depth, DataType targetType) {
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      if (!field.name().equals(path[depth])) {
        continue;
      }
      StructField[] newFields = fields.clone();
      if (depth == path.length - 1) {
        newFields[i] =
            new StructField(field.name(), targetType, field.nullable(), field.metadata());
      } else if (field.dataType() instanceof StructType) {
        StructType nested =
            setFieldType((StructType) field.dataType(), path, depth + 1, targetType);
        newFields[i] = new StructField(field.name(), nested, field.nullable(), field.metadata());
      } else {
        return schema;
      }
      return new StructType(newFields);
    }
    return schema;
  }

  /** Rebuilds the struct(s) along {@code path} so the leaf field is cast to double. */
  private static Dataset<Row> castFieldToDouble(Dataset<Row> df, String[] path) {
    Column castedLeaf = col(String.join(".", path)).cast(DataTypes.DoubleType);
    if (path.length == 1) {
      return df.withColumn(path[0], castedLeaf);
    }
    return df.withColumn(path[0], withFieldCast(col(path[0]), path, 1, castedLeaf));
  }

  private static Column withFieldCast(Column current, String[] path, int depth, Column castedLeaf) {
    if (depth == path.length - 1) {
      return current.withField(path[depth], castedLeaf);
    }
    return current.withField(
        path[depth], withFieldCast(current.getField(path[depth]), path, depth + 1, castedLeaf));
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
