package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.*;
import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.TableBuildPipeline.createExtensionTable;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;
import static org.gbif.pipelines.spark.util.TableUtil.verbatimExtensionTableName;

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
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DcElement;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.jetbrains.annotations.NotNull;

/**
 * Pipeline that populates verbatim extension tables. This is called from TableBuildPipeline but is
 * left as a runnable here for convenience.
 */
@Slf4j
public class VerbatimExtensionsInterpretationPipeline {

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

    @Parameter(names = CONFIG_PATH_ARG, description = "Path to YAML configuration file")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(names = SPARK_MASTER_ARG, description = "Spark master - there for local dev only")
    private String master;

    @Parameter(names = NUMBER_OF_SHARDS_ARG, description = "Number of shards")
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
      SparkSession spark, DatasetType datasetType, String inputPath, String datasetId) {

    String verbatimDirectory =
        datasetType == DatasetType.OCCURRENCE
            ? Directories.VERBATIM_EXT_FILTERED
            : Directories.VERBATIM_EVENT_EXT_FILTERED;

    // Load extended records that have been filtered to only allowed extensions
    Dataset<ExtendedRecord> extendedRecords =
        spark
            .read()
            .parquet(inputPath + "/" + verbatimDirectory)
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
    log.debug(
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
      DatasetType datasetType) {

    String inputPath = String.format("%s/%s/%d", config.getInputPath(), datasetId, attempt);

    registerUdfs(spark);

    // Normalize the directory name
    Dataset<Row> normalizedKeys = readExtendedRecords(spark, datasetType, inputPath, datasetId);

    // Dynamically create flattened columns
    Dataset<Row> flattened = selectColumnsForExtension(spark, normalizedKeys);

    // Repartition for performance
    spark.sparkContext().setJobGroup("repartitioning", "Repartitioning by extension", true);
    Dataset<Row> optimized = flattened.repartition(col("directory"));

    log.info(String.join(", ", optimized.columns()));

    // collect distinct directories
    List<String> directories =
        optimized.select(col("directory")).distinct().as(Encoders.STRING()).collectAsList();

    log.debug(
        "Found {} distinct extension directories: {}",
        directories.size(),
        String.join(", ", directories));

    // cache columns for later use
    Set<String> dfCols = new HashSet<>(Arrays.asList(optimized.columns()));

    // Check if we can create the table schema
    Map<String, ExtensionTable> extensionTableMap =
        ExtensionTable.tableExtensions().stream()
            .collect(Collectors.toMap(ExtensionTable::getHiveTableName, et -> et));

    log.debug("Available extension tables: {}", String.join(", ", extensionTableMap.keySet()));

    // Write partitioned Parquet output (flat schema)
    for (String dir : directories) {

      String normalisedDir = dir.toLowerCase().replaceAll("_", "");
      // find the supported extension for this directory name
      ExtensionTable extensionTable = extensionTableMap.get(normalisedDir);
      if (extensionTable == null) {
        log.warn(
            "Directory '{}' does not match any supported extension, skipping processing for this directory",
            dir);
        continue;
      }

      String dwcCoreTerm = datasetType == DatasetType.OCCURRENCE ? "occurrence" : "event";

      String table =
          icebergCatalog
              + "."
              + verbatimExtensionTableName(
                  extensionTable, dwcCoreTerm); // e.g. iceberg_catalog.default.ac_extension

      if (!spark.catalog().tableExists(table)) {
        log.info(
            "ExtensionTable found for directory '{}' normalised '{}', table schema can be created: {}",
            dir,
            normalisedDir,
            extensionTable.getSchema());
        // creating table
        createExtensionTable(config.getTableBuildConfig(), spark, extensionTable, dwcCoreTerm);
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
      log.info("Selecting {}", Arrays.stream(colsToSelect).toList());

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

    // create a map of normalized column names to original column names
    // for matching against the table schema
    //    Map<String, String> dfColsNormalised =
    //        dfCols.stream().collect(Collectors.toMap(c -> c.toLowerCase().replaceAll("_", ""), c
    // -> c));

    for (StructField structField : tblSchema.fields()) {
      String fieldName = structField.name();
      // look for verbatim fields
      if (fieldName.startsWith("v_")) {
        String nonVerbatimFieldName = fieldName.substring(2);
        if (dfCols.contains(nonVerbatimFieldName)) {
          //          String dfFieldName = dfColsNormalised.get(nonVerbatimFieldName);
          colsToSelect.add(col(nonVerbatimFieldName).alias(fieldName));
        }
      } else if (dfCols.contains(fieldName)) {
        colsToSelect.add(col(fieldName));
        //      } else if (dfColsNormalised.containsKey(fieldName)) {
        //        String originalFieldName = dfColsNormalised.get(fieldName);
        //        colsToSelect.add(col(originalFieldName).alias(fieldName));
      } else {
        colsToSelect.add(lit(null).cast(structField.dataType()).alias(fieldName));
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
    String prefix = "";
    if (!rawName.equalsIgnoreCase(DcTerm.identifier.simpleName())) {
      if (name.startsWith(DcTerm.identifier.namespace().toString())) {
        prefix = DcTerm.identifier.prefix() + "_";
      } else if (name.startsWith(DcElement.identifier.namespace().toString())) {
        prefix = DcElement.identifier.prefix() + "_";
      }
    }
    return prefix + rawName.toLowerCase().trim();
  }

  @SneakyThrows
  private static Extension lookupExtension(String rowType) {
    return Extension.fromRowType(rowType);
  }

  public static void main(String[] argsv) throws Exception {
    VerbatimExtensionsInterpretationPipeline.Args args =
        new VerbatimExtensionsInterpretationPipeline.Args();
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
        getSparkSession(args.master, args.appName, config, TableBuildPipeline::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);
    /* ############ standard init block - end ########## */

    processExtensions(spark, config, datasetId, attempt, config.getHiveDB(), args.datasetType);

    fileSystem.close();
    spark.stop();
    spark.close();
    System.exit(0);
  }
}
