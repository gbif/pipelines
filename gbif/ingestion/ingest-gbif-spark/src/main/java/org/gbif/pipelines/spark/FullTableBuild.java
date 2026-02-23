package org.gbif.pipelines.spark;

import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.coordinator.DistributedUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.gbif.pipelines.IngestUtils;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class FullTableBuild {

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
        names = "--sourceDirectory",
        description = "Directory containing the parquet to load",
        required = true)
    private String sourceDirectory = "hdfs";

    @Parameter(
        names = "--coreDwcTerm",
        description = "Core Darwin Core term to build the table for, e.g. 'occurrence' or 'event'",
        required = true)
    private String coreDwcTerm = "occurrence";

    @Parameter(
        names = "--unsuccessfulDumpFilename",
        description =
            "Filename to dump the list of unsuccessful datasets to in HDFS for later review",
        required = true)
    private String unsuccessfulDumpFilename = "unsuccessful-iceberg-datasets.txt";

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  public static void main(String[] argsv) throws Exception {
    FullTableBuild.Args args = new FullTableBuild.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    long start = System.currentTimeMillis();

    PipelinesConfig config = loadConfig(args.config);

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master, "Rebuild Occurrence Table", config, TableBuild::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */

    IngestUtils.DirectoryScanResult scanResult =
        IngestUtils.getSuccessFulParquetFilePaths(
            fileSystem,
            config,
            args.sourceDirectory,
            config.getRebuildPath() + "/" + args.unsuccessfulDumpFilename);

    spark.udf().register("base64_decode", new TableBuild.Base64DecodeUDF(), DataTypes.StringType);

    log.info("Starting table build");

    // load hdfs view
    Dataset<Row> hdfs =
        spark
            .read()
            .parquet(scanResult.successfulPaths().toArray(new String[0]))
            .coalesce(args.numberOfShards);

    String tempLoadingTable =
        String.format("%s_%s_%d", args.coreDwcTerm, "rebuild", System.currentTimeMillis());

    // Switch to the configured Hive database
    spark.sql("USE " + config.getHiveDB());

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

    Dataset<Row> df =
        spark.sql(
            "SELECT COUNT(*) AS cnt FROM "
                + tempLoadingTable
                + " WHERE datasetKey IS NULL"
                + " OR datasetKey = ''"
                + " OR gbifId IS NULL"
                + " OR gbifId = ''");
    long count = df.collectAsList().get(0).getLong(0);
    if (count > 0) {
      log.warn(
          "There are {} records with NULL or empty datasetKey or gbifId in the temporary table {}",
          count,
          tempLoadingTable);
      throw new IllegalStateException("There are " + count + " records with NULL datasetKey");
    }

    if (spark.catalog().tableExists(args.coreDwcTerm)) {
      log.info("Table {} exists", args.coreDwcTerm);
    } else {
      log.info("Table {} does not exist and will be created", args.coreDwcTerm);

      // Create or populate the occurrence table SQL
      spark.sql(getCreateTableSQL(args.coreDwcTerm));

      log.info("Table {} created. Creating extension tables", args.coreDwcTerm);
    }

    // get the hdfs columns from the parquet with mappings to iceberg columns
    Map<String, TableBuild.HdfsColumn> hdfsColumnList = getHdfsColumns(hdfs);

    // Read the target table i.e. 'occurrence' or 'event' schema to ensure it exists
    StructType tblSchema = spark.read().format("iceberg").load(args.coreDwcTerm).schema();

    // Build the insert query
    String insertQuery =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s (%s) SELECT %s FROM %s.%s",
            config.getHiveDB(),
            args.coreDwcTerm,
            Arrays.stream(tblSchema.fields())
                .map(StructField::name)
                .collect(Collectors.joining(", ")),
            generateSelectColumns(tblSchema, hdfsColumnList),
            config.getHiveDB(),
            tempLoadingTable);

    log.debug("Inserting data into {} table: {}", args.coreDwcTerm, insertQuery);

    // Execute the insert
    spark.sql(insertQuery);

    // Drop the temporary table
    spark.sql("DROP TABLE " + tempLoadingTable);

    log.info(timeAndRecPerSecond("full-tablebuild", start, avroToHdfsCountAttempted));
  }

  private static String generateSelectColumns(
      StructType tblSchema, Map<String, TableBuild.HdfsColumn> hdfsColumnList) {
    return Arrays.stream(tblSchema.fields())
        .map(
            structField -> {
              TableBuild.HdfsColumn hdfsColumn = hdfsColumnList.get(structField.name());
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
  private static Map<String, TableBuild.HdfsColumn> getHdfsColumns(Dataset<Row> hdfs) {

    // get the hdfs columns from the parquet and map them to select statements
    Map<String, TableBuild.HdfsColumn> hdfsColumnList = new HashMap<>();

    for (String parquetColumn : hdfs.columns()) {

      TableBuild.HdfsColumn hdfsColumn = new TableBuild.HdfsColumn();

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

  static String getFieldDefns() {
    return OccurrenceHDFSTableDefinition.definition().stream()
        .map(field -> field.getHiveField() + " " + field.getHiveDataType())
        .collect(Collectors.joining(", \n"));
  }

  public static String getCreateTableSQL(String tableName) {
    return String.format(
        """
          CREATE TABLE IF NOT EXISTS %s
          (%s)
          USING iceberg
          PARTITIONED BY (datasetkey)
          TBLPROPERTIES (
            'write.format.default'='parquet',
            'parquet.compression'='SNAPPY',
            'auto.purge'='true'
          )
        """,
        tableName, getFieldDefns());
  }
}
