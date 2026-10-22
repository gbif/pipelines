package org.gbif.pipelines.spark;

import static org.gbif.pipelines.core.utils.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.util.LogUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;
import static org.gbif.pipelines.spark.util.TableUtil.*;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.TableBuildConfig;
import org.gbif.pipelines.spark.pojo.HdfsColumn;
import org.gbif.pipelines.spark.util.SingleDatasetPipelineArgs;
import org.gbif.pipelines.spark.util.TableUtil;
import org.jetbrains.annotations.NotNull;

/**
 * This pipeline loads the /hdfs directory for a dataset/attempt, creates a temporary table with the
 * parquet, and then loads into the main table which is partitioned by datasetKey.
 *
 * <p>This is used for occurrence and events.
 */
@Slf4j
public class TableBuildPipeline {

  private static final Object LOCK = new Object();

  @Parameters(separators = "=")
  private static class Args extends SingleDatasetPipelineArgs {

    @Parameter(
        names = DATASET_TYPE_ARG,
        description = "OCCURRENCE or SAMPLING_EVENT",
        required = true)
    private DatasetType datasetType;

    @Parameter(
        names = SOURCE_DIRECTORY_ARG,
        description = "Directory containing parquet to load",
        required = true)
    private String sourceDirectory = "hdfs";
  }

  public static void main(String[] argsv) throws Exception {
    TableBuildPipeline.Args args = new TableBuildPipeline.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true); // FIXME to ease airflow/registry integration
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    String datasetId = args.datasetId;
    int attempt = args.attempt;
    SparkSession spark = null;
    FileSystem fileSystem = null;

    try {
      /* ############ standard init block ########## */
      spark =
          getSparkSession(
              args.master, args.appName, config, TableBuildPipeline::configSparkSession);
      fileSystem = getFileSystem(spark, config);

      /* ############ standard init block - end ########## */
      if (args.datasetType != DatasetType.OCCURRENCE
          && args.datasetType != DatasetType.SAMPLING_EVENT) {
        throw new IllegalArgumentException("Invalid dataset type: " + args.datasetType);
      }

      runTableBuild(
          spark, fileSystem, config, args.datasetType, datasetId, attempt, args.sourceDirectory);
    } finally {
      if (fileSystem != null) {
        fileSystem.close();
      }
      if (spark != null) {
        spark.stop();
        spark.close();
      }
    }
    System.exit(0);
  }

  public static void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    sparkBuilder
        .enableHiveSupport()
        .config("spark.hadoop.hive.metastore.uris", config.getHiveMetastoreUris());
  }

  /**
   * Run an incremental table build for the supplied dataset
   *
   * @param spark Spark session
   * @param fileSystem HDFS file system
   * @param config Pipelines config
   * @param datasetId Dataset ID
   * @param attempt Attempt number
   * @param sourceDirectory Source directory of parquet files
   */
  public static <T> void runTableBuild(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      DatasetType datasetType,
      String datasetId,
      int attempt,
      String sourceDirectory)
      throws Exception {

    String coreDwcTerm = datasetType == DatasetType.OCCURRENCE ? "occurrence" : "event";

    long start = System.currentTimeMillis();
    ThreadContext.put("datasetKey", datasetId);
    ThreadContext.put("attempt", String.valueOf(attempt));
    ThreadContext.put(
        "step",
        datasetType == DatasetType.OCCURRENCE
            ? StepType.HDFS_VIEW.name()
            : StepType.EVENTS_HDFS_VIEW.name());

    log.info("Starting table build");

    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    // load hdfs view
    Dataset<Row> hdfs = spark.read().parquet(outputPath + "/" + sourceDirectory);

    // Generate a unique temporary view name. The parquet is read directly through a session
    // temp view rather than being copied into an intermediate Iceberg table first.
    String tempCoreView =
        String.format("%s_%s_%d", coreDwcTerm, datasetId.replace("-", "_"), attempt);
    hdfs.createOrReplaceTempView(tempCoreView);

    // Switch to the configured Hive database
    spark.sql("USE " + config.getHiveDB());

    if (log.isDebugEnabled()) {
      spark.sql("DESCRIBE TABLE " + tempCoreView).show(false);
    }

    // count records and check for records without gbifId or datasetKey in a single scan
    long avroToHdfsCountAttempted = checkForOrphanedRecord(spark, tempCoreView);

    if (spark.catalog().tableExists(coreDwcTerm)) {
      log.debug("Table {} exists", coreDwcTerm);
    } else {
      log.info("Table {} does not exist and will be created", coreDwcTerm);

      // Create or populate the core table SQL
      spark.sql(getCreateTableSQL(config.getTableBuildConfig(), datasetType, coreDwcTerm));

      log.info("Table {} created. Creating extension tables", coreDwcTerm);

      // create extension tables dynamically if they do not exist
      for (ExtensionTable extTable : ExtensionTable.tableExtensions()) {
        String extTableName = extTable.getHiveTableName();
        if (spark.catalog().tableExists(extTableName)) {
          log.info("Extension table {} exists", extTableName);
        } else {
          log.info("Extension table {} does not exist and will be created", extTableName);
          createExtensionTable(config.getTableBuildConfig(), spark, extTable, coreDwcTerm);
        }
      }
    }

    // get the hdfs columns from the parquet with mappings to iceberg columns
    Map<String, HdfsColumn> hdfsColumnList = getHdfsColumns(hdfs);

    // May need to use zookeeper or similar for distributed locking
    // The lock solves problems with standalone CLIs
    synchronized (LOCK) {
      // Read the target table i.e. 'occurrence' or 'event' schema to ensure it exists
      StructType tblSchema = spark.read().format("iceberg").load(coreDwcTerm).schema();

      // Build the select query, columns are in the target table order and aliased to its names
      String selectQuery =
          String.format(
              "SELECT %s FROM %s", generateSelectColumns(tblSchema, hdfsColumnList), tempCoreView);

      String targetTable = config.getHiveDB() + "." + coreDwcTerm;
      log.debug("Inserting data into {} table: {}", coreDwcTerm, selectQuery);
      TableUtil.runWithRetries(
          spark,
          () ->
              overwritePartitions(
                  spark.sql(selectQuery),
                  targetTable,
                  config.getTableBuildConfig().getWriteDistributionMode()),
          targetTable,
          config.getTableBuildConfig().getMaxRetriesWithRefresh(),
          config.getTableBuildConfig().getBaseSleepMs());
    }

    // process verbatim extensions
    VerbatimExtensionsInterpretationPipeline.processExtensions(
        spark, config, datasetId, attempt, config.getHiveDB(), datasetType);

    // Create multimedia table if it does not exist
    if (!spark.catalog().tableExists(coreDwcTerm + "_multimedia")) {
      log.info("Multimedia table does not exist and will be created");
      spark.sql(getCreateMultimediaTableSQL(config.getTableBuildConfig(), coreDwcTerm));
    }

    // write to the multimedia table
    insertOverwriteMultimediaTableFromTemp(spark, tempCoreView, coreDwcTerm + "_multimedia");

    if (datasetType == DatasetType.OCCURRENCE) {
      // Create dna table if it does not exist
      String dnaTableName = coreDwcTerm + "_dna_derived_data";
      if (!spark.catalog().tableExists(dnaTableName)) {
        log.info("DNA derived data table does not exist and will be created");
        spark.sql(getCreateDnaDerivedDataTableSQL(config.getTableBuildConfig(), dnaTableName));
      }

      // write to the dna table
      insertOverwriteDnaDerivedDataTable(spark, tempCoreView, dnaTableName, true);
    }

    // if a sampling event dataset, create the humboldt_event table if it does not exist and
    // populate it
    if (datasetType == DatasetType.SAMPLING_EVENT) {
      String humboldtTableName = coreDwcTerm + "_humboldt";
      if (!spark.catalog().tableExists(humboldtTableName)) {
        // populate the event table
        spark.sql(getCreateIfNotExistsHumboldt(config.getTableBuildConfig(), humboldtTableName));
      }
      insertOverwriteHumboldtTableFromTemp(spark, tempCoreView, humboldtTableName);
    }

    // Drop the temporary view
    spark.catalog().dropTempView(tempCoreView);
    log.debug("Dropped temporary view: {}", tempCoreView);

    // Write metrics to yaml
    writeMetricsYaml(
        fileSystem,
        Map.of("avroToHdfsCountAttempted", avroToHdfsCountAttempted),
        outputPath + "/" + getMetricsFileName(coreDwcTerm));

    log.info(timeAndRecPerSecond("tablebuild", start, avroToHdfsCountAttempted));
  }

  public static void createExtensionTable(
      TableBuildConfig config,
      SparkSession spark,
      ExtensionTable extensionTable,
      String coreDwcTerm) {
    log.info("Create extension table: {}", verbatimExtensionTableName(extensionTable, coreDwcTerm));
    spark
        .sparkContext()
        .setJobDescription("Create " + verbatimExtensionTableName(extensionTable, coreDwcTerm));

    String extensionTableSql = createVerbatimExtensionTableSQL(config, extensionTable, coreDwcTerm);
    log.info("Creating extension table SQL {}", extensionTableSql);
    spark.sql(extensionTableSql);
  }

  @NotNull
  public static String getMetricsFileName(String tableName) {
    return tableName + "-to-hdfs.yml";
  }
}
