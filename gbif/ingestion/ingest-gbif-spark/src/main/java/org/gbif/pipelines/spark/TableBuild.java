package org.gbif.pipelines.spark;

import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.coordinator.DistributedUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.SparkUtil.getSparkSession;
import static org.gbif.pipelines.spark.TableUtil.*;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.udf.CleanDelimiterCharsUdf;
import org.jetbrains.annotations.NotNull;

/**
 * This pipeline loads the /hdfs directory for a dataset/attempt, creates a temporary table with the
 * parquet, and then loads into the main occurrence table which is partitioned by datasetKey
 */
@Slf4j
public class TableBuild {

  private static final Object LOCK = new Object();

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(names = "--core", description = "Table name", required = true)
    private String tableName = "occurrence";

    @Parameter(names = "--sourceDirectory", description = "Table name", required = true)
    private String sourceDirectory = "hdfs";

    @Parameter(names = "--config", description = "Path to YAML configuration file")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = "--master",
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
    TableBuild.Args args = new TableBuild.Args();
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
        getSparkSession(args.master, args.appName, config, TableBuild::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */
    DatasetType datasetType;
    switch (args.tableName.toLowerCase()) {
      case "occurrence":
        datasetType = DatasetType.OCCURRENCE;
        break;
      case "event":
        datasetType = DatasetType.SAMPLING_EVENT;
        break;
      default:
        log.error("Invalid coreDwcTerm: {}. Supported values are: {}");
        jCommander.usage();
        return;
    }

    runTableBuild(
        spark,
        fileSystem,
        config,
        datasetType,
        datasetId,
        attempt,
        args.tableName,
        args.sourceDirectory);

    spark.stop();
    spark.close();
    fileSystem.close();
    System.exit(0);
  }

  public static void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    sparkBuilder
        .enableHiveSupport()
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.warehouse.dir", "hdfs://gbif-hdfs/stackable/warehouse")

        // FIXME move to config
        .config("spark.sql.catalog.local.commit.retry.num-retries", "10")
        .config("spark.sql.catalog.local.commit.retry.min-wait-ms", "100")
        .config("spark.sql.catalog.local.commit.retry.max-wait-ms", "1000")
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
   * @param coreDwcTerm table name e.g. occurrence or event
   * @param sourceDirectory Source directory of parquet files
   */
  public static <T> void runTableBuild(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      DatasetType datasetType,
      String datasetId,
      int attempt,
      String coreDwcTerm,
      String sourceDirectory)
      throws Exception {

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

      // Create or populate the occurrence table SQL
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
    VerbatimExtensionsInterpretation.processExtensions(
        spark, config, datasetId, attempt, config.getHiveDB(), coreDwcTerm);

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
      if (!spark.catalog().tableExists(coreDwcTerm + "_multimedia")) {
        // populate the humboldt_event table
        spark.sql(getCreateIfNotExistsHumboldt(coreDwcTerm));
      }
      insertOverwriteHumboldtTableFromTemp(spark, tempCoreTable, coreDwcTerm + "_humboldt");
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

  private static String generateSelectColumns(
      StructType tblSchema, Map<String, HdfsColumn> hdfsColumnList) {
    return Arrays.stream(tblSchema.fields())
        .map(
            structField -> {
              HdfsColumn hdfsColumn = hdfsColumnList.get(structField.name());
              if (hdfsColumn != null) {
                return hdfsColumn.select;
              } else {
                // Column not found in HDFS, select NULL with alias
                return "NULL AS `" + structField.name() + "`";
              }
            })
        .collect(Collectors.joining(", "));
  }

  /**
   * Creates a map of HDFS columns with their corresponding Iceberg column names and select
   * statements.
   *
   * @param hdfs parquet dataset
   * @return map of HDFS columns
   */
  @NotNull
  private static Map<String, HdfsColumn> getHdfsColumns(Dataset<Row> hdfs) {

    // get the hdfs columns from the parquet and map them to select statements
    Map<String, HdfsColumn> hdfsColumnList = new HashMap<>();

    for (String parquetColumn : hdfs.columns()) {

      HdfsColumn hdfsColumn = new HdfsColumn();

      // normalize column names
      final String normalisedName = parquetColumn.toLowerCase().replace("$", "");

      if (parquetColumn.equalsIgnoreCase("extMultimedia")) {

        hdfsColumn.icebergCol = "ext_multimedia";
        hdfsColumn.select = "base64_decode(extMultimedia) AS `ext_multimedia`";

      } else if (parquetColumn.equalsIgnoreCase("extHumboldt")) {

        hdfsColumn.icebergCol = "ext_humboldt";
        hdfsColumn.select = "base64_decode(extHumboldt) AS `ext_humboldt`";

      } else if (parquetColumn.matches("^[vV][A-Z].*")) {

        // Handles names like VSomething → v_something
        String normalized = "v_" + normalisedName;
        hdfsColumn.icebergCol = normalized;
        hdfsColumn.select = "`" + parquetColumn + "` AS " + normalized;

      } else {

        hdfsColumn.icebergCol = normalisedName;
        hdfsColumn.select = "`" + parquetColumn + "` AS " + normalisedName;
      }

      hdfsColumnList.put(hdfsColumn.icebergCol, hdfsColumn);
      log.debug(
          "Mapped HDFS column '{}' to Iceberg column '{}' with select '{}'",
          parquetColumn,
          hdfsColumn.icebergCol,
          hdfsColumn.select);
    }

    return hdfsColumnList;
  }

  @NotNull
  public static String getMetricsFileName(String tableName) {
    return tableName + "-to-hdfs.yml";
  }

  @NotNull
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

  @ToString
  static class HdfsColumn {
    String select;
    String icebergCol;
  }

  public static class Base64DecodeUDF implements UDF1<String, String> {

    @Override
    public String call(String encoded) throws Exception {
      if (encoded == null) {
        return null;
      }

      try {
        byte[] decodedBytes = Base64.getDecoder().decode(encoded);
        return new String(decodedBytes, StandardCharsets.UTF_8);
      } catch (IllegalArgumentException e) {
        // Invalid Base64 input
        return null;
      }
    }
  }
}
