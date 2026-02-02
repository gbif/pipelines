package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.*;
import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.Directories.VERBATIM_EXT_FILTERED;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.SparkUtil.getSparkSession;
import static org.gbif.pipelines.spark.TableBuild.createExtensionTable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.*;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class VerbatimExtensionsInterpretation {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(names = "--icebergCatalog", description = "Apache Iceberg Catalog", required = true)
    private String icebergCatalog;

    @Parameter(names = "--dwcCoreTerm", description = "DarwinCore Core Term", required = true)
    private String dwcCoreTerm;

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

    @Parameter(names = "--numberOfShards", description = "Number of shards", required = false)
    private int numberOfShards = 10;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  /** Register UDFs used in the processing. */
  private static void registerUdfs(SparkSession spark) {
    spark
        .udf()
        .register(
            "extensionToDirectory",
            (String url) -> extensionToDirectory(url),
            DataTypes.StringType);
  }

  /** Read extended records and explode extensions into separate rows. */
  private static Dataset<Row> readExtendedRecords(
      SparkSession spark, String inputPath, String datasetId) {

    // Load extended records that have been filtered to only allowed extensions
    Dataset<ExtendedRecord> extendedRecords =
        spark
            .read()
            .parquet(inputPath + "/" + VERBATIM_EXT_FILTERED)
            .as(Encoders.bean(ExtendedRecord.class));

    spark.sparkContext().setJobGroup("explode-extensions", "Exploding extensions", true);
    // Convert to DataFrame and rename id to gbifid
    Dataset<Row> df = extendedRecords.toDF().withColumnRenamed("id", "gbifid");

    // Explode extensions into separate rows
    Dataset<Row> exploded =
        df.select(col("gbifid"), col("coreId"), expr("explode(extensions) as (directory, records)"))
            .withColumn("record", explode(col("records")))
            .withColumn("datasetKey", lit(datasetId))
            .drop("records");

    // Normalize the directory name
    return exploded.withColumn("directory", callUDF("extensionToDirectory", col("directory")));
  }

  /** Dynamically create flattened columns based on all keys in the 'record' map. */
  private static Dataset<Row> selectColumnsForExtension(SparkSession spark, Dataset<Row> df) {
    spark.sparkContext().setJobGroup("select-ext-columns", "Selecting columns of extension", true);
    Dataset<Row> keysDf = df.select(explode(map_keys(col("record"))).alias("key"));

    List<String> allKeys = keysDf.distinct().as(Encoders.STRING()).collectAsList();

    // Dynamically create flattened columns
    List<Column> selectCols = new ArrayList<>();
    selectCols.add(col("gbifid"));
    selectCols.add(col("coreId"));
    selectCols.add(col("directory"));

    for (String k : allKeys) {
      String normalizedField = normalizeFieldName(k);
      selectCols.add(col("record").getItem(k).alias(normalizedField));
    }
    log.info(
        "Selecting [{}] columns for extension",
        selectCols.stream().map(Column::toString).collect(Collectors.joining(", ")));

    return df.select(selectCols.toArray(new Column[0]));
  }

  @SneakyThrows
  public static void processExtensions(
      SparkSession spark,
      PipelinesConfig config,
      String datasetId,
      int attempt,
      String icebergCatalog,
      String dwcCoreTerm) {

    String inputPath = String.format("%s/%s/%d", config.getInputPath(), datasetId, attempt);

    registerUdfs(spark);

    // Normalize the directory name
    Dataset<Row> normalizedKeys = readExtendedRecords(spark, inputPath, datasetId);

    // Dynamically create flattened columns
    Dataset<Row> flattened = selectColumnsForExtension(spark, normalizedKeys);

    // Repartition for performance
    spark.sparkContext().setJobGroup("repartitioning", "Repartitioning by extension", true);
    Dataset<Row> optimized = flattened.repartition(col("directory"));

    // collect distinct directories
    List<String> directories =
        optimized.select(col("directory")).distinct().as(Encoders.STRING()).collectAsList();

    log.info(
        "Found {} distinct extension directories: {}",
        directories.size(),
        String.join(", ", directories));

    // cache columns for later use
    Set<String> dfCols = new HashSet<>(Arrays.asList(optimized.columns()));

    // Write partitioned Parquet output (flat schema)
    for (String dir : directories) {
      String tableName =
          dwcCoreTerm.toLowerCase() + "_ext_" + dir.toLowerCase(); // e.g. ac_extension -> extension
      String table = icebergCatalog + "." + tableName; // e.g. iceberg_catalog.default.ac_extension

      if (!spark.catalog().tableExists(table)) {
        log.info("Table {} does not exist, checking if we should create {}", table, dir);

        // Check if we can create the table schema
        Map<String, ExtensionTable> extensionTableMap =
            ExtensionTable.tableExtensions().stream()
                .collect(Collectors.toMap(ExtensionTable::getHiveTableName, et -> et));

        if (!extensionTableMap.containsKey(dir)) {
          log.warn("No ExtensionTable found for directory '{}', will not create table schema", dir);
          continue;
        } else {
          ExtensionTable extTable = extensionTableMap.get(dir);
          log.info(
              "ExtensionTable found for directory '{}', table schema can be created: {}",
              dir,
              extTable.getSchema());
          // creating table
          createExtensionTable(spark, extTable, dwcCoreTerm);
        }
      }

      log.info("Processing extension directory '{}' into table '{}'", dir, table);
      spark
          .sparkContext()
          .setJobGroup("extension-" + table, "Loading extension data into table " + table, true);

      // get target table schema (table must exist)
      StructType tblSchema = spark.read().format("iceberg").load(table).schema();

      // build select list that matches target schema: use existing columns or nulls cast to the
      // target type
      var colsToSelect = getColsToSelect(tblSchema, dfCols);

      // filter rows for this extension and select aligned columns
      Dataset<Row> toWrite =
          optimized
              .filter(col("directory").equalTo(dir)) // select data in the extension
              .select(colsToSelect); // align columns to target schema

      // ensure partition column exists in the DataFrame if the table is partitioned by datasetKey
      if (!Arrays.asList(toWrite.columns()).contains("datasetKey")) {
        toWrite = toWrite.withColumn("datasetKey", lit(datasetId));
      }

      // write to existing Iceberg table (append; use overwritePartitions() if needed)
      toWrite.writeTo(table).overwritePartitions();
    }
  }

  /**
   * Builds a list of columns to select from the DataFrame, aligning with the target table schema.
   */
  @NotNull
  private static Column[] getColsToSelect(StructType tblSchema, Set<String> dfCols) {
    List<Column> colsToSelect = new ArrayList<>();
    for (StructField f : tblSchema.fields()) {
      String fieldName = f.name();
      if (dfCols.contains(fieldName)) {
        colsToSelect.add(col(fieldName));
      } else {
        colsToSelect.add(lit(null).cast(f.dataType()).alias(fieldName));
      }
    }
    return colsToSelect.toArray(new Column[0]);
  }

  /** Converts extension rowType URL to directory name. */
  private static String extensionToDirectory(String rowType) {
    Extension extension = lookupExtension(rowType);
    return extension.name().toLowerCase();
  }

  /** Extracts the last part of the url as the field name and normalizes it. */
  private static String normalizeFieldName(String name) {
    String[] parts = name.split("/");
    String rawName = parts[parts.length - 1];
    return rawName.trim();
  }

  @SneakyThrows
  private static Extension lookupExtension(String rowType) {
    return Extension.fromRowType(rowType);
  }

  public static void main(String[] argsv) throws Exception {
    VerbatimExtensionsInterpretation.Args args = new VerbatimExtensionsInterpretation.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
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

    processExtensions(spark, config, datasetId, attempt, args.icebergCatalog, args.dwcCoreTerm);

    fileSystem.close();
    spark.stop();
    spark.close();
    System.exit(0);
  }
}
