package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.coordinator.DistributedUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.SparkUtil.getSparkSession;
import static org.gbif.terms.utils.TermUtils.INTERPRETED_HUMBOLDT_TERMS;

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
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.download.hive.HiveDataTypes;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.gbif.pipelines.IngestUtils;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.udf.CleanDelimiterCharsUdf;
import org.jetbrains.annotations.NotNull;

/**
 * This class performs a full rebuild of the Iceberg table from the parquet files in HDFS. It reads
 * the parquet files, maps the columns to the Iceberg schema, and writes the data to a temporary
 * Iceberg table. Then it inserts the data from the temporary table into the final Iceberg table
 * (e.g. 'occurrence' or 'event') and drops the temporary table. It also checks for any records with
 * NULL or empty datasetKey or gbifId and throws an exception if any are found.
 */
@Slf4j
public class FullTableBuild {

  public static final List<String> SUPPORTED_CORE_TERMS = List.of("occurrence", "event");

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
        description = "Directory containing the parquet to load e.g. hdfs, json, etc",
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
        names = "--earliestModificationTime",
        description =
            "Only consider parquet files modified after this time (ISO 8601 format, e.g. 2024-01-01T00:00:00Z)")
    private String earliestModificationTime = null;

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

    if (args.coreDwcTerm == null
        || args.coreDwcTerm.isEmpty()
        || !SUPPORTED_CORE_TERMS.contains(args.coreDwcTerm)) {
      log.error("coreDwcTerm is required and cannot be empty");
      jCommander.usage();
      return;
    }

    long start = System.currentTimeMillis();

    PipelinesConfig config = loadConfig(args.config);

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master, "Rebuild iceberg tables - " + args.coreDwcTerm, config, TableBuild::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */

    IngestUtils.DirectoryScanResult scanResult =
        IngestUtils.getSuccessFulParquetFilePaths(
            fileSystem,
            config,
            args.sourceDirectory,
            config.getRebuildPath() + "/" + args.unsuccessfulDumpFilename,
            args.earliestModificationTime);

    // For testing - hardcode the scan result to avoid hitting HDFS and speed up development.
    // The paths should be to the directories containing the parquet files, not the parquet files
    // themselves.
    //    IngestUtils.DirectoryScanResult scanResult =
    //        new IngestUtils.DirectoryScanResult(
    //            List.of(
    //
    // "hdfs://gbif-hdfs/data/ingest_lab/34039ff3-7eaf-4ebf-bcf4-64b7365004b4/1/hdfs/",
    //
    // "hdfs://gbif-hdfs/data/ingest_lab/cc63e998-fe1b-468d-94f1-6afcf494d0e4/1/hdfs/",
    //
    // "hdfs://gbif-hdfs/data/ingest_lab/100a9054-e46c-4302-b059-3a8836df1cf7/1/hdfs/",
    //
    // "hdfs://gbif-hdfs/data/ingest_lab/f08c9244-5af4-458c-8966-43d981da09e7/1/hdfs/"),
    //            Map.of(
    //                "34039ff3-7eaf-4ebf-bcf4-64b7365004b4",
    //                1,
    //                "cc63e998-fe1b-468d-94f1-6afcf494d0e4",
    //                1,
    //                "100a9054-e46c-4302-b059-3a8836df1cf7",
    //                1,
    //                "f08c9244-5af4-458c-8966-43d981da09e7",
    //                1));

    spark.udf().register("base64_decode", new TableBuild.Base64DecodeUDF(), DataTypes.StringType);
    spark.udf().register("cleanDelimiters", new CleanDelimiterCharsUdf(), DataTypes.StringType);

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

    String prefix = "rebuild_" + start + "_";

    // Create the occurrence table SQL
    spark.sql(getCreateTableSQL(prefix, args.coreDwcTerm));

    // get the hdfs columns from the parquet with mappings to iceberg columns
    Map<String, TableBuild.HdfsColumn> hdfsColumnList = getHdfsColumns(hdfs);

    // Read the target table i.e. 'occurrence' or 'event' schema to ensure it exists
    StructType tblSchema = spark.read().format("iceberg").load(args.coreDwcTerm).schema();

    // Build the insert query
    String insertQuery =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s%s (%s) SELECT %s FROM %s.%s",
            config.getHiveDB(),
            prefix,
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

    // Create occurrence_multimedia table
    spark.sql(getCreateMultimediaTableSQL(prefix, args.coreDwcTerm));

    // Insert multimedia data into the occurrence_multimedia table
    insertOverwriteMultimediaTable(
        spark, prefix + args.coreDwcTerm, prefix + args.coreDwcTerm + "_multimedia");

    if (args.coreDwcTerm.equalsIgnoreCase("event")) {
      // For event table, also create the event_humboldt table and insert data
      // Create event_humboldt table
      String tableName = prefix + "event_humboldt";
      spark.sql(getCreateIfNotExistsHumboldt(tableName));
      insertOverwriteHumboldtTable(spark, tableName);
    }

    log.info(timeAndRecPerSecond("full-table-build", start, avroToHdfsCountAttempted));
  }

  public static void insertOverwriteMultimediaTable(
      SparkSession spark, String occurrenceTable, String multimediaTable) {
    spark
        .table(occurrenceTable)
        .select(
            col("gbifid"),
            from_json(
                    col("ext_multimedia"),
                    new ArrayType(
                        new StructType()
                            .add("type", "string", false)
                            .add("format", "string", false)
                            .add("identifier", "string", false)
                            .add("references", "string", false)
                            .add("title", "string", false)
                            .add("description", "string", false)
                            .add("source", "string", false)
                            .add("audience", "string", false)
                            .add("created", "string", false)
                            .add("creator", "string", false)
                            .add("contributor", "string", false)
                            .add("publisher", "string", false)
                            .add("license", "string", false)
                            .add("rightsHolder", "string", false),
                        true))
                .alias("mm_record"),
            col("datasetkey"))
        .select(col("gbifid"), explode(col("mm_record")).alias("mm_record"), col("datasetkey"))
        .select(
            col("gbifid"),
            callUDF("cleanDelimiters", col("mm_record.type")).alias("type"),
            callUDF("cleanDelimiters", col("mm_record.format")).alias("format"),
            callUDF("cleanDelimiters", col("mm_record.identifier")).alias("identifier"),
            callUDF("cleanDelimiters", col("mm_record.references")).alias("references"),
            callUDF("cleanDelimiters", col("mm_record.title")).alias("title"),
            callUDF("cleanDelimiters", col("mm_record.description")).alias("description"),
            callUDF("cleanDelimiters", col("mm_record.source")).alias("source"),
            callUDF("cleanDelimiters", col("mm_record.audience")).alias("audience"),
            col("mm_record.created").alias("created"),
            callUDF("cleanDelimiters", col("mm_record.creator")).alias("creator"),
            callUDF("cleanDelimiters", col("mm_record.contributor")).alias("contributor"),
            callUDF("cleanDelimiters", col("mm_record.publisher")).alias("publisher"),
            callUDF("cleanDelimiters", col("mm_record.license")).alias("license"),
            callUDF("cleanDelimiters", col("mm_record.rightsHolder")).alias("rightsHolder"),
            col("datasetkey"))
        .createOrReplaceTempView("mm_records");

    spark.sql(
        String.format(
            """
                    INSERT OVERWRITE TABLE %s
                    SELECT
                        gbifid,
                        type,
                        format,
                        identifier,
                        references,
                        title,
                        description,
                        source,
                        audience,
                        created,
                        creator,
                        contributor,
                        publisher,
                        license,
                        rightsHolder,
                        datasetkey
                    FROM mm_records
                    """,
            multimediaTable));
  }

  public static String getCreateIfNotExistsHumboldt(String tableName) {

    String selectTerms =
        INTERPRETED_HUMBOLDT_TERMS.stream()
            .map(term -> term.simpleName() + " " + HiveDataTypes.typeForTerm(term, false))
            .collect(Collectors.joining(","));

    return String.format(
        """
                 CREATE TABLE IF NOT EXISTS %s
                 (gbifid STRING, %s, datasetkey STRING) "
                 STORED AS PARQUET
                 TBLPROPERTIES (
                    'write.format.default' = 'parquet',
                    'parquet.compression' = 'ZSTD',
                    'auto.purge' = 'true',
                    'write.merge.isolation-level' = 'snapshot',
                    'commit.retry.num-retries' = '10',
                    'commit.retry.min-wait-ms' = '1000',
                    'commit.retry.max-wait-ms' = '10000'
                 )
            """,
        tableName, selectTerms);
  }

  public static void insertOverwriteHumboldtTable(SparkSession spark, String tableName) {
    spark
        .table(tableName)
        .select(
            col("gbifid"),
            from_json(
                    col("ext_humboldt"),
                    new ArrayType(
                        createHumboldtStructTypeFromJson(INTERPRETED_HUMBOLDT_TERMS), true))
                .alias("h_record"))
        .select(col("gbifid"), explode(col("h_record")).alias("h_record"))
        .createOrReplaceTempView("h_records");

    String interpretedTerms =
        INTERPRETED_HUMBOLDT_TERMS.stream()
            .map(t -> "h_record." + t.simpleName())
            .collect(Collectors.joining(","));

    spark.sql(
        String.format(
            """
                      INSERT OVERWRITE TABLE %s
                      SELECT gbifid, %s, datasetkey FROM h_records
                    """,
            tableName, interpretedTerms));
  }

  private static StructType createHumboldtStructTypeFromJson(List<Term> terms) {
    StructType structType = new StructType();
    for (Term humboldtTerm : terms) {
      String hiveDataType = HiveDataTypes.typeForTerm(humboldtTerm, false);
      DataType type = null;
      switch (hiveDataType) {
        case HiveDataTypes.TYPE_STRING:
          type = DataTypes.StringType;
          break;
        case HiveDataTypes.TYPE_ARRAY_STRING:
          type = new ArrayType(DataTypes.StringType, true);
          break;
        case HiveDataTypes.TYPE_INT:
          type = DataTypes.IntegerType;
          break;
        case HiveDataTypes.TYPE_DOUBLE:
          type = DataTypes.DoubleType;
          break;
        case HiveDataTypes.TYPE_BOOLEAN:
          type = DataTypes.BooleanType;
          break;
        case HiveDataTypes.TYPE_MAP_OF_MAP_ARRAY_STRUCT:
          type =
              new MapType(
                  DataTypes.StringType,
                  new MapType(
                      DataTypes.StringType, new ArrayType(DataTypes.StringType, true), true),
                  true);
          break;
        case HiveDataTypes.TYPE_VOCABULARY_ARRAY_STRUCT:
          type =
              new StructType()
                  .add("concepts", new ArrayType(DataTypes.StringType, true))
                  .add("lineage", new ArrayType(DataTypes.StringType, true));
          break;
      }

      if (type != null) {
        structType = structType.add(humboldtTerm.simpleName(), type, true);
      } else {
        log.warn("Type not found for humboldt term {}", humboldtTerm);
      }
    }

    return structType;
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

  static String getFieldDefinitions() {
    return OccurrenceHDFSTableDefinition.definition().stream()
        .map(field -> field.getHiveField() + " " + field.getHiveDataType())
        .collect(Collectors.joining(", \n"));
  }

  public static String getCreateTableSQL(String prefix, String tableName) {
    return String.format(
        """
          CREATE TABLE IF NOT EXISTS %s%s
          (%s)
          USING iceberg
          PARTITIONED BY (datasetkey)
          TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'parquet.compression' = 'ZSTD',
            'auto.purge' = 'true',
            'write.merge.isolation-level' = 'snapshot',
            'commit.retry.num-retries' = '10',
            'commit.retry.min-wait-ms' = '1000',
            'commit.retry.max-wait-ms' = '10000'
          )
        """,
        prefix, tableName, getFieldDefinitions());
  }

  public static String getCreateMultimediaTableSQL(String prefix, String coreDwcTerm) {
    return String.format(
        """
          CREATE TABLE IF NOT EXISTS %s%s_multimedia
          (
             gbifid STRING,
             type STRING,
             format STRING,
             identifier STRING,
             references STRING,
             title STRING,
             description STRING,
             source STRING,
             audience STRING,
             created STRING,
             creator STRING,
             contributor STRING,
             publisher STRING,
             license STRING,
             rightsHolder STRING,
             datasetkey STRING
          )
          USING iceberg
          PARTITIONED BY (datasetkey)
          TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'parquet.compression' = 'ZSTD',
            'auto.purge' = 'true',
            'write.merge.isolation-level' = 'snapshot',
            'commit.retry.num-retries' = '10',
            'commit.retry.min-wait-ms' = '1000',
            'commit.retry.max-wait-ms' = '10000'
          )
        """,
        prefix, coreDwcTerm);
  }
}
