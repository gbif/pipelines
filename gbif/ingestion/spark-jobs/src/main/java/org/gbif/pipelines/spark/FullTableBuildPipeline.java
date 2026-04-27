package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.util.FullBuildUtils.checkDatasetTypeSupported;
import static org.gbif.pipelines.spark.util.LogUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;
import static org.gbif.pipelines.spark.util.TableUtil.*;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.pojo.HdfsColumn;
import org.gbif.pipelines.spark.udf.Base64DecodeUDF;
import org.gbif.pipelines.spark.udf.CleanDelimiterCharsUdf;
import org.gbif.pipelines.spark.util.FullBuildUtils;

/**
 * This class performs a full rebuild of the Iceberg table from the parquet files in HDFS. It reads
 * the parquet files, maps the columns to the Iceberg schema, and writes the data to a temporary
 * Iceberg table. Then it inserts the data from the temporary table into the final Iceberg table
 * (e.g. 'occurrence' or 'event') and drops the temporary table. It also checks for any records with
 * NULL or empty datasetKey or gbifId and throws an exception if any are found.
 */
@Slf4j
public class FullTableBuildPipeline {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = CONFIG_PATH_ARG, description = "Path to YAML configuration file")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = SPARK_MASTER_ARG,
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(names = NUMBER_OF_SHARDS_ARG, description = "Number of shards")
    private int numberOfShards = 2400;

    @Parameter(
        names = SOURCE_DIRECTORY_ARG,
        description = "Directory containing the parquet to load e.g. hdfs, json, etc",
        required = true)
    private String sourceDirectory = "hdfs";

    @Parameter(names = DATASET_TYPE_ARG, description = "OCCURRENCE or SAMPLING_EVENT")
    private DatasetType datasetType = DatasetType.OCCURRENCE;

    @Parameter(
        names = UNSUCCESSFUL_DUMP_FILENAME,
        description =
            "Filename to dump the list of unsuccessful datasets to in HDFS for later review",
        required = true)
    private String unsuccessfulDumpFilename = "unsuccessful-iceberg-datasets.txt";

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
        names = "--existingTempLoadingTable",
        description =
            "Existing Temporary loading table. Can be used for re-runs where the load step fails")
    private String existingTempLoadingTable = null;

    @Parameter(
        names = "--existingCoreOutputTable",
        description =
            "Existing Core output loading table. Can be used for re-runs where the load step fails")
    private String existingCoreOutputTable = null;

    @Parameter(names = "--loadCoreTable", description = "Load core table")
    private boolean loadCoreTable = true;

    @Parameter(names = "--loadMultimediaTable", description = "Load multimedia table")
    private boolean loadMultimediaTable = true;

    @Parameter(names = "--loadHumboldtTable", description = "Load humboldt table")
    private boolean loadHumboldtTable = true;

    @Parameter(names = "--skipEbird", description = "Skip ebird")
    private boolean skipEbird = false;

    @Parameter(
        names = "--dropTempTableOnSuccess",
        description =
            "Drop the temporary loading table after successful load into final tables. "
                + "Should be true for normal runs, but can be set to false for "
                + "debugging to keep the temp table around for inspection.")
    private boolean dropTempTableOnSuccess = true;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  public static void main(String[] argsv) throws Exception {
    FullTableBuildPipeline.Args args = new FullTableBuildPipeline.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    // check dataset type supported
    checkDatasetTypeSupported(args.datasetType);

    long start = System.currentTimeMillis();

    PipelinesConfig config = loadConfig(args.config);

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master,
            "Rebuild iceberg tables - " + args.datasetType,
            config,
            TableBuildPipeline::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */

    spark.udf().register("base64_decode", new Base64DecodeUDF(), DataTypes.StringType);
    spark.udf().register("cleanDelimiters", new CleanDelimiterCharsUdf(), DataTypes.StringType);

    log.info("Starting table build");

    FullBuildUtils.DirectoryScanResult scanResult =
        FullBuildUtils.getSuccessfulParquetFilePaths(
            fileSystem,
            config,
            args.sourceDirectory,
            config.getRebuildPath() + "/" + args.unsuccessfulDumpFilename,
            args.earliestModificationTime);

    // load hdfs view
    Dataset<Row> hdfs =
        spark
            .read()
            .parquet(scanResult.successfulPaths().toArray(new String[0]))
            .coalesce(args.numberOfShards);

    String coreDwcTerm = args.datasetType == DatasetType.OCCURRENCE ? "occurrence" : "event";

    // Switch to the configured Hive database
    spark.sql("USE " + config.getHiveDB());

    Long avroToHdfsCountAttempted = -1L;
    String tempLoadingTable = null;


    if (args.existingTempLoadingTable != null && !args.existingTempLoadingTable.isEmpty()) {
      log.info("Using provided temp loading table: {}", args.existingTempLoadingTable);
      tempLoadingTable = args.existingTempLoadingTable;
      avroToHdfsCountAttempted =
          spark.sql("SELECT COUNT(*) FROM " + tempLoadingTable).collectAsList().get(0).getLong(0);
      log.info(
          "Count of records in provided temp loading table {} is {}",
          tempLoadingTable,
          avroToHdfsCountAttempted);
    } else {
      tempLoadingTable =
          String.format("%s_%s_%d", coreDwcTerm, "rebuild", System.currentTimeMillis());

      log.info("No temp loading table provided, will create table {}", tempLoadingTable);
      avroToHdfsCountAttempted = loadTempLoadingTable(spark, tempLoadingTable, hdfs);
      log.info(
          "Loaded data into temp loading table {} with {} records",
          tempLoadingTable,
          avroToHdfsCountAttempted);
    }

    String prefix = null;

    if (args.loadCoreTable) {
      log.info("Loading Core Table");

      String targetTable = null;

      if (args.existingCoreOutputTable != null && !args.existingCoreOutputTable.isEmpty()) {
        targetTable = args.existingCoreOutputTable;
        prefix = targetTable.replace(coreDwcTerm, "");
        log.info("Using provided core output table: {}", targetTable);
      } else {
        prefix = "rebuild_" + System.currentTimeMillis() + "_";
        // Create the occurrence table SQL
        spark.sql(
            getCreateTableSQL(config.getTableBuildConfig(), args.datasetType, prefix, coreDwcTerm));
        targetTable = config.getHiveDB() + "." + prefix + coreDwcTerm;
      }

      // get the hdfs columns from the parquet with mappings to iceberg columns
      Map<String, HdfsColumn> hdfsColumnList = getHdfsColumns(hdfs);

      // Read the target table i.e. 'occurrence' or 'event' schema to ensure it exists
      StructType tblSchema =
          spark
              .read()
              .format("iceberg")
              .load(targetTable)
              .schema();

      String sourceTable = config.getHiveDB() + "." + tempLoadingTable;

      // -------------------do eBird first
      if (!args.skipEbird) {
        spark.sparkContext().setJobGroup("insert-into-core", "Loading eBird into core", true);
        // Build the insert query
        String insertEbirdQuery =
            String.format(
                "INSERT INTO TABLE %s (%s) SELECT %s FROM %s WHERE datasetkey = '4fa7b334-ce0d-4e88-aaae-2e0c138d049e'",
                targetTable,
                Arrays.stream(tblSchema.fields())
                    .map(StructField::name)
                    .collect(Collectors.joining(", ")),
                generateSelectColumns(tblSchema, hdfsColumnList),
                sourceTable);

        log.debug("Inserting data into {} table: {}", coreDwcTerm, insertEbirdQuery);

        // Execute the insert
        spark.sql(insertEbirdQuery);
      }

      // -------------------do rest

      spark
          .sparkContext()
          .setJobGroup("insert-into-core", "Loading everything else into core", true);
      // Build the insert query
      String insertAllElseQuery =
          String.format(
              "INSERT INTO TABLE %s (%s) SELECT %s FROM %s WHERE datasetkey != '4fa7b334-ce0d-4e88-aaae-2e0c138d049e'",
              targetTable,
              Arrays.stream(tblSchema.fields())
                  .map(StructField::name)
                  .collect(Collectors.joining(", ")),
              generateSelectColumns(tblSchema, hdfsColumnList),
              sourceTable);

      log.debug("Inserting data into {} table: {}", coreDwcTerm, insertAllElseQuery);

      // Execute the insert
      spark.sql(insertAllElseQuery);

      if (args.dropTempTableOnSuccess) {
        log.info("Dropping temporary loading table {}", tempLoadingTable);
        // Drop the temporary table
        spark.sql("DROP TABLE " + tempLoadingTable);
      } else {
        log.info("Not dropping temporary loading table {}", tempLoadingTable);
      }
    } else {
      log.warn("Skipping load of core table as the flag is set to false.");
    }

    if (args.loadMultimediaTable) {
      log.info("Loading Multimedia Table");
      spark
          .sparkContext()
          .setJobGroup("insert-into-multimedia", "Loading everything else into multimedia", true);
      // Create occurrence_multimedia table
      spark.sql(getCreateMultimediaTableSQL(config.getTableBuildConfig(), prefix, coreDwcTerm));

      // Insert multimedia data into the occurrence_multimedia table
      insertOverwriteMultimediaTable(
          spark, prefix + coreDwcTerm, prefix + coreDwcTerm + "_multimedia");
    } else {
      log.warn("Skipping load of Multimedia Table as the flag is set to false.");
    }

    // if its the event table, also create the event_humboldt table and insert data
    if (args.loadHumboldtTable && coreDwcTerm.equalsIgnoreCase("event")) {
      log.info("Loading Humboldt Table");
      spark
          .sparkContext()
          .setJobGroup("insert-into-multimedia", "Loading everything else into humboldt", true);
      // For event table, also create the event_humboldt table and insert data
      // Create event_humboldt table
      String tableName = prefix + "event_humboldt";
      spark.sql(getCreateIfNotExistsHumboldt(config.getTableBuildConfig(), tableName));
      insertOverwriteHumboldtTable(spark, prefix + "event", tableName);
    } else {
      log.info(
          "Skipping load of Humboldt Table as the flag is set to false or the dataset type is not event.");
    }

    log.info("Renaming tables to final names if the flag is set: {}", args.switchOnSuccess);
    if (args.switchOnSuccess) {
      switchLiveTables(coreDwcTerm, spark, config, prefix);
    }

    log.info(timeAndRecPerSecond("full-table-build", start, avroToHdfsCountAttempted));
  }

  private static long loadTempLoadingTable(
      SparkSession spark, String tempLoadingTable, Dataset<Row> hdfs) {
    // Drop the table if it already exists
    spark.sql("DROP TABLE IF EXISTS " + tempLoadingTable);

    log.debug("Creating temp loading Iceberg table: {}", tempLoadingTable);

    // Check HDFS for remnant DB files from failed attempts
    hdfs.writeTo(tempLoadingTable).create();

    log.debug("Created Iceberg table: {}", tempLoadingTable);

    // Display table schema and initial record count
    Dataset<Row> result = spark.sql("SELECT COUNT(*) FROM " + tempLoadingTable);
    long avroToHdfsCountAttempted = result.collectAsList().get(0).getLong(0);

    if (log.isDebugEnabled()) {
      spark.sql("DESCRIBE TABLE " + tempLoadingTable).show(false);
      spark.sql("SELECT COUNT(*) FROM " + tempLoadingTable).show(false);
    }

    checkForOrphanedRecord(spark, tempLoadingTable);
    return avroToHdfsCountAttempted;
  }

  /**
   * Switch the new interpreted tables to the final names (e.g. 'occurrence' or 'event') after
   * successful build.
   *
   * @param coreDwcTerm the core Darwin Core term, e.g. 'occurrence' or 'event'
   * @param spark the SparkSession
   * @param config the PipelinesConfig
   * @param prefix the prefix used for the new tables, e.g. 'rebuild_1234567890_'
   */
  private static void switchLiveTables(
      String coreDwcTerm, SparkSession spark, PipelinesConfig config, String prefix) {
    // Rename tables to final names
    List<String> tables = new ArrayList<>();
    tables.add(coreDwcTerm);
    tables.add(coreDwcTerm + "_multimedia");
    if (coreDwcTerm.equalsIgnoreCase("event")) {
      tables.add("event_humboldt");
    }

    // rename old tables if they exist, and drop old_ tables if they exist
    for (String table : tables) {

      if (spark.catalog().tableExists(table)) {

        log.info("Final table {} already exists and will be overwritten", table);
        // remove the old table if it exists
        if (spark.catalog().tableExists("old_" + table)) {
          log.info("Old table old_{} already exists and will be dropped", table);
          spark.sql(String.format("DROP TABLE %s.%s", config.getHiveDB(), "old_" + table));
        }

        // rename the current table to old_table
        log.info("Renaming existing table {} to old_{}", table, table);
        spark.sql(
            String.format(
                "ALTER TABLE %s.%s RENAME TO %s", config.getHiveDB(), table, "old_" + table));
      }

      // rename the new table to the final name
      log.info("Renaming table {} to final name {}", prefix + table, table);
      spark.sql(
          String.format(
              "ALTER TABLE %s.%s RENAME TO %s", config.getHiveDB(), prefix + table, table));
    }
  }
}
