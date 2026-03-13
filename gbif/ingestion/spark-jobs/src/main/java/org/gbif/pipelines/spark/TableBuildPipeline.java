package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.util.LogUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.spark.util.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;
import static org.gbif.pipelines.spark.util.TableUtil.*;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.pojo.HdfsColumn;
import org.gbif.pipelines.spark.udf.Base64DecodeUDF;
import org.gbif.pipelines.spark.udf.CleanDelimiterCharsUdf;
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
  private static class Args {

    @Parameter(names = APP_NAME_ARG, description = "Application name", required = true)
    private String appName;

    @Parameter(names = DATASET_ID_ARG, description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = ATTEMPT_ID_ARG, description = "Attempt number", required = true)
    private int attempt;

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

    @Parameter(names = CONFIG_PATH_ARG, description = "Path to YAML configuration file")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = SPARK_MASTER_ARG,
        description =
            "Spark master - there for local dev only. Use --master=local[*] to run locally.",
        required = false)
    private String master;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
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

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(args.master, args.appName, config, TableBuildPipeline::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */
    if (args.datasetType != DatasetType.OCCURRENCE
        && args.datasetType != DatasetType.SAMPLING_EVENT) {
      throw new IllegalArgumentException("Invalid dataset type: " + args.datasetType);
    }

    runTableBuild(
        spark, fileSystem, config, args.datasetType, datasetId, attempt, args.sourceDirectory);

    spark.stop();
    spark.close();
    fileSystem.close();
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

    spark.udf().register("base64_decode", new Base64DecodeUDF(), DataTypes.StringType);
    spark.udf().register("cleanDelimiters", new CleanDelimiterCharsUdf(), DataTypes.StringType);

    long start = System.currentTimeMillis();
    ThreadContext.put("datasetKey", datasetId);
    log.info("Starting table build");

    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    // load hdfs view
    Dataset<Row> hdfs = spark.read().parquet(outputPath + "/" + sourceDirectory);

    // Generate a unique temporary table name
    String tempCoreTable =
        String.format("%s_%s_%d", coreDwcTerm, datasetId.replace("-", "_"), attempt);

    // Switch to the configured Hive database
    spark.sql("USE " + config.getHiveDB());

    // Drop the table if it already exists
    spark.sql("DROP TABLE IF EXISTS " + tempCoreTable);

    // Check HDFS for remnant DB files from failed attempts
    cleanHdfsPath(fileSystem, config, tempCoreTable);
    hdfs.writeTo(tempCoreTable).create();

    log.debug("Created Iceberg table: {}", tempCoreTable);

    // Display table schema and initial record count
    Dataset<Row> result = spark.sql("SELECT COUNT(*) FROM " + tempCoreTable);
    long avroToHdfsCountAttempted = result.collectAsList().get(0).getLong(0);

    if (log.isDebugEnabled()) {
      spark.sql("DESCRIBE TABLE " + tempCoreTable).show(false);
      spark.sql("SELECT COUNT(*) FROM " + tempCoreTable).show(false);
    }

    // check for records without gbifId or datasetKey
    checkForOrphanedRecord(spark, tempCoreTable);

    if (spark.catalog().tableExists(coreDwcTerm)) {
      log.debug("Table {} exists", coreDwcTerm);
    } else {
      log.info("Table {} does not exist and will be created", coreDwcTerm);

      // Create or populate the core table SQL
      spark.sql(getCreateTableSQL(datasetType, coreDwcTerm));

      log.info("Table {} created. Creating extension tables", coreDwcTerm);

      // create extension tables dynamically if they do not exist
      for (ExtensionTable extTable : ExtensionTable.tableExtensions()) {
        String extTableName = extTable.getHiveTableName();
        if (spark.catalog().tableExists(extTableName)) {
          log.info("Extension table {} exists", extTableName);
        } else {
          log.info("Extension table {} does not exist and will be created", extTableName);
          createExtensionTable(spark, extTable, coreDwcTerm);
        }
      }
    }

    // get the hdfs columns from the parquet with mappings to iceberg columns
    Map<String, HdfsColumn> hdfsColumnList = getHdfsColumns(hdfs);

    // FIXME - limit concurrent writes to the iceberg table
    // May need to use zookeeper or similar for distributed locking
    synchronized (LOCK) {

      // Read the target table i.e. 'occurrence' or 'event' schema to ensure it exists
      StructType tblSchema = spark.read().format("iceberg").load(coreDwcTerm).schema();

      // Build the insert query
      String insertQuery =
          String.format(
              "INSERT OVERWRITE TABLE %s.%s (%s) SELECT %s FROM %s.%s",
              config.getHiveDB(),
              coreDwcTerm,
              Arrays.stream(tblSchema.fields())
                  .map(StructField::name)
                  .collect(Collectors.joining(", ")),
              generateSelectColumns(tblSchema, hdfsColumnList),
              config.getHiveDB(),
              tempCoreTable);

      log.debug("Inserting data into {} table: {}", coreDwcTerm, insertQuery);

      // Execute the insert
      spark.sql(insertQuery);
    }

    // process verbatim extensions
    VerbatimExtensionsInterpretationPipeline.processExtensions(
        spark, config, datasetId, attempt, config.getHiveDB(), datasetType);

    // Create multimedia table if it does not exist
    if (!spark.catalog().tableExists(coreDwcTerm + "_multimedia")) {
      log.info("Multimedia table does not exist and will be created");
      spark.sql(getCreateMultimediaTableSQL(coreDwcTerm));
    }

    // write to the multimedia table
    insertOverwriteMultimediaTableFromTemp(spark, tempCoreTable, coreDwcTerm + "_multimedia");

    // if a sampling event dataset, create the humboldt_event table if it does not exist and
    // populate it
    if (datasetType == DatasetType.SAMPLING_EVENT) {
      String humboldtTableName = coreDwcTerm + "_humboldt";
      if (!spark.catalog().tableExists(humboldtTableName)) {
        // populate the event table
        spark.sql(getCreateIfNotExistsHumboldt(humboldtTableName));
      }
      insertOverwriteHumboldtTableFromTemp(spark, tempCoreTable, humboldtTableName);
    }

    // Drop the temporary table
    spark.sql("DROP TABLE " + tempCoreTable);

    log.debug("Dropped Iceberg table: {}", tempCoreTable);
    cleanHdfsPath(fileSystem, config, tempCoreTable);

    // Write metrics to yaml
    writeMetricsYaml(
        fileSystem,
        Map.of("avroToHdfsCountAttempted", avroToHdfsCountAttempted),
        outputPath + "/" + getMetricsFileName(coreDwcTerm));

    log.info(timeAndRecPerSecond("tablebuild", start, avroToHdfsCountAttempted));
  }

  public static void createExtensionTable(
      SparkSession spark, ExtensionTable extensionTable, String coreDwcTerm) {
    log.info("Create extension table: {}", verbatimExtensionTableName(extensionTable, coreDwcTerm));
    spark
        .sparkContext()
        .setJobDescription("Create " + verbatimExtensionTableName(extensionTable, coreDwcTerm));

    String extensionTableSql = createVerbatimExtensionTableSQL(extensionTable, coreDwcTerm);
    log.info("Creating extension table SQL {}", extensionTableSql);
    spark.sql(extensionTableSql);
  }

  @NotNull
  public static String getMetricsFileName(String tableName) {
    return tableName + "-to-hdfs.yml";
  }

  private static void cleanHdfsPath(FileSystem fileSystem, PipelinesConfig config, String table)
      throws IOException {
    Path warehousePath = new Path(config.getHdfsWarehousePath() + "/" + table);
    log.debug("Checking warehouse path: {}", warehousePath);
    if (fileSystem.exists(warehousePath)) {
      log.debug("Deleting warehouse path: {}", warehousePath);
      fileSystem.delete(warehousePath, true);
      log.debug("Deleted warehouse path: {}", warehousePath);
    }
  }
}
