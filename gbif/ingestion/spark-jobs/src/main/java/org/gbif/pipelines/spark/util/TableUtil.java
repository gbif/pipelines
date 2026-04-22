package org.gbif.pipelines.spark.util;

import static org.apache.spark.sql.functions.*;
import static org.gbif.terms.utils.TermUtils.INTERPRETED_HUMBOLDT_TERMS;

import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.download.hive.EventHDFSTableDefinition;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.occurrence.download.hive.HiveDataTypes;
import org.gbif.occurrence.download.hive.InitializableField;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.gbif.pipelines.core.config.model.TableBuildConfig;
import org.gbif.pipelines.spark.pojo.HdfsColumn;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class TableUtil {

  public static final Set<String> EXTENSION_COLUMNS = Set.of("ext_multimedia", "ext_humboldt");

  public static void main(String[] args) {

    TableBuildConfig config = new TableBuildConfig();

    System.out.println(getCreateTableSQL(config, DatasetType.SAMPLING_EVENT, "", "event"));
    System.out.println(getCreateTableSQL(config, DatasetType.OCCURRENCE, "", "occurrence"));
  }

  /**
   * Check for records without a datasetKey or gbifId.
   *
   * @param spark
   * @param tempLoadingTable
   */
  public static void checkForOrphanedRecord(SparkSession spark, String tempLoadingTable) {
    Dataset<Row> df =
        spark.sql(
            String.format(
                """
                    SELECT COUNT(*) AS cnt
                    FROM %s
                    WHERE datasetKey IS NULL
                      OR datasetKey = ''
                      OR gbifId IS NULL
                      OR gbifId = ''
                """,
                tempLoadingTable));
    long count = df.collectAsList().get(0).getLong(0);
    if (count > 0) {
      log.warn(
          "There are {} records with NULL or empty datasetKey or gbifId in the temporary table {}",
          count,
          tempLoadingTable);
      throw new IllegalStateException("There are " + count + " records with NULL datasetKey");
    }
  }

  public static String getCreateTableSQL(
      TableBuildConfig config, DatasetType datasetType, String prefix, String tableName) {
    return String.format(
        """
          CREATE TABLE IF NOT EXISTS %s%s
          (%s)
          USING iceberg
          PARTITIONED BY (datasetkey)
          TBLPROPERTIES (%s)
        """,
        prefix, tableName, getFieldDefinitions(datasetType), generateTblProperties(config));
  }

  static String getFieldDefinitions(DatasetType datasetType) {
    List<InitializableField> definition;

    if (datasetType == DatasetType.OCCURRENCE) {
      definition = OccurrenceHDFSTableDefinition.definition();
    } else if (datasetType == DatasetType.SAMPLING_EVENT) {
      definition = EventHDFSTableDefinition.definition();
    } else {
      throw new IllegalArgumentException("Unsupported dataset type: " + datasetType);
    }

    return definition.stream()
        .filter(field -> !field.getTerm().equals(GbifTerm.datasetKey))
        .map(field -> field.getHiveField() + " " + field.getHiveDataType())
        .collect(Collectors.joining(", \n")
     ) + ", datasetkey STRING";
  }

  public static String getCreateMultimediaTableSQL(
      TableBuildConfig config, String prefix, String coreDwcTerm) {
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
          TBLPROPERTIES (%s)
        """,
        prefix, coreDwcTerm, generateTblProperties(config));
  }

  /**
   * Creates a map of HDFS columns with their corresponding Iceberg column names and select
   * statements.
   *
   * @param hdfs parquet dataset
   * @return map of HDFS columns
   */
  @NotNull
  public static Map<String, HdfsColumn> getHdfsColumns(Dataset<Row> hdfs) {

    // get the hdfs columns from the parquet and map them to select statements
    Map<String, HdfsColumn> hdfsColumnList = new HashMap<>();

    for (String parquetColumn : hdfs.columns()) {

      HdfsColumn hdfsColumn = getHdfsColumn(parquetColumn);
      hdfsColumnList.put(hdfsColumn.getIcebergCol(), hdfsColumn);
      log.debug(
          "Mapped HDFS column '{}' to Iceberg column '{}' with select '{}'",
          parquetColumn,
          hdfsColumn.getIcebergCol(),
          hdfsColumn.getSelect());
    }

    return hdfsColumnList;
  }

  private static HdfsColumn getHdfsColumn(String parquetColumn) {
    HdfsColumn hdfsColumn = new HdfsColumn();

    // normalize column names
    final String normalisedName = parquetColumn.toLowerCase().replace("$", "");

    if (EXTENSION_COLUMNS.contains(normalisedName)) {

      hdfsColumn.setIcebergCol(parquetColumn);
      hdfsColumn.setSelect(
          String.format("base64_decode(%s) AS `%s`", parquetColumn, normalisedName));

    } else if (normalisedName.equalsIgnoreCase("class_")) {

      hdfsColumn.setIcebergCol("class");
      hdfsColumn.setSelect("`" + parquetColumn + "` AS class");

    } else {

      hdfsColumn.setIcebergCol(normalisedName);
      hdfsColumn.setSelect("`" + parquetColumn + "` AS " + normalisedName);
    }
    return hdfsColumn;
  }

  public static String getCreateTableSQL(
      TableBuildConfig config, DatasetType datasetType, String tableName) {
    return getCreateTableSQL(config, datasetType, "", tableName);
  }

  public static String getCreateMultimediaTableSQL(TableBuildConfig config, String coreDwcTerm) {
    return String.format(
        """
          CREATE TABLE IF NOT EXISTS %s_multimedia
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
          TBLPROPERTIES (%s)
        """,
        coreDwcTerm, generateTblProperties(config));
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
            callUDF("cleanDelimiters", col("mm_record.rightsHolder")).alias("rightsholder"),
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
                   rightsholder,
                   datasetkey
               FROM mm_records
            """,
            multimediaTable));
  }

  public static void insertOverwriteHumboldtTableFromTemp(
      SparkSession spark, String sourceTableName, String humboldtTable) {
    spark
        .table(sourceTableName)
        .select(
            col("gbifid"),
            from_json(
                    expr("cast(base64_decode(ext_humboldt) as string)"),
                    new ArrayType(
                        createHumboldtStructTypeFromJson(INTERPRETED_HUMBOLDT_TERMS), true))
                .alias("h_record"),
            col("datasetkey"))
        .select(col("gbifid"), explode(col("h_record")).alias("h_record"), col("datasetkey"))
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
            humboldtTable, interpretedTerms));
  }

  public static void insertOverwriteMultimediaTableFromTemp(
      SparkSession spark, String tempOccurrenceTable, String multimediaTable) {
    spark
        .table(tempOccurrenceTable)
        .select(
            col("gbifid"),
            from_json(
                    expr("cast(base64_decode(ext_multimedia) as string)"),
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
            callUDF("cleanDelimiters", col("mm_record.rightsHolder")).alias("rightsholder"),
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
                   rightsholder,
                   datasetkey
               FROM mm_records
            """,
            multimediaTable));
  }

  public static String getCreateIfNotExistsHumboldt(TableBuildConfig config, String tableName) {

    String selectTerms =
        INTERPRETED_HUMBOLDT_TERMS.stream()
            .map(term -> term.simpleName() + " " + HiveDataTypes.typeForTerm(term, false))
            .collect(Collectors.joining(","));

    return String.format(
        """
             CREATE TABLE IF NOT EXISTS %s
             (gbifid STRING, %s, datasetkey STRING)
             USING iceberg
             PARTITIONED BY (datasetkey)
             TBLPROPERTIES (%s)
        """,
        tableName, selectTerms, generateTblProperties(config));
  }

  private static Object generateTblProperties(TableBuildConfig config) {
    return String.format(
        """
              'write.format.default' = '%s',
              'parquet.compression' = '%s',
              'auto.purge' = '%s',
              'write.merge.isolation-level' = '%s',
              'commit.retry.num-retries' = '%s',
              'commit.retry.min-wait-ms' = '%s',
              'commit.retry.max-wait-ms' = '%s',
              'history.expire.max-snapshot-age-ms' = '%s',
              'history.expire.min-snapshots-to-keep' = '%s'
            """,
        config.getWrite_format_default(),
        config.getParquet_compression(),
        config.getAuto_purge(),
        config.getWrite_merge_isolation_level(),
        config.getCommit_retry_num_retries(),
        config.getCommit_retry_min_wait_ms(),
        config.getCommit_retry_max_wait_ms(),
        config.getHistory_expire_max_snapshot_age_ms(),
        config.getHistory_expire_min_snapshots_to_keep());
  }

  public static void insertOverwriteHumboldtTable(
      SparkSession spark, String sourceTableName, String targetTableName) {
    spark
        .table(sourceTableName)
        .select(
            col("gbifid"),
            from_json(
                    col("ext_humboldt"),
                    new ArrayType(
                        createHumboldtStructTypeFromJson(INTERPRETED_HUMBOLDT_TERMS), true))
                .alias("h_record"),
            col("datasetkey"))
        .select(col("gbifid"), explode(col("h_record")).alias("h_record"), col("datasetkey"))
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
            targetTableName, interpretedTerms));
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

  public static String generateSelectColumns(
      StructType tblSchema, Map<String, HdfsColumn> hdfsColumnList) {
    return Arrays.stream(tblSchema.fields())
        .map(
            structField -> {
              HdfsColumn hdfsColumn = hdfsColumnList.get(structField.name());
              if (hdfsColumn != null) {
                return hdfsColumn.getSelect();
              } else {
                // Column not found in HDFS, select NULL with alias
                return "NULL AS `" + structField.name() + "`";
              }
            })
        .collect(Collectors.joining(", "));
  }

  public static String createVerbatimExtensionTableSQL(
      TableBuildConfig config, ExtensionTable extensionTable, String coreDwcTerm) {

    // generate field list
    String fieldList =
        extensionTable.getSchema().getFields().stream()
            .map(f -> f.name() + " STRING")
            .collect(Collectors.joining(",\n "));

    return String.format(
        """
        CREATE TABLE IF NOT EXISTS %s (
        %s
        )
        USING iceberg
        PARTITIONED BY (datasetkey)
        TBLPROPERTIES (
        %s)
        """,
        verbatimExtensionTableName(extensionTable, coreDwcTerm),
        fieldList,
        generateTblProperties(config));
  }

  public static String createMigrationVerbatimExtensionTableSQL(
      TableBuildConfig config,
      String sourceSchema,
      String targetSchema,
      ExtensionTable extensionTable,
      String coreDwcTerm) {

    // generate field list
    String fieldList =
        extensionTable.getSchema().getFields().stream()
            .map(f -> "`" + f.name() + "`")
            .collect(Collectors.joining(",\n "));
    return String.format(
        """
            INSERT INTO %s.%s (%s)
            SELECT %s FROM %s.%s
            """,
        targetSchema,
        verbatimExtensionTableName(extensionTable, coreDwcTerm),
        fieldList,
        fieldList,
        sourceSchema,
        verbatimExtensionTableName(extensionTable, coreDwcTerm));
  }

  public static String verbatimExtensionTableName(
      ExtensionTable extensionTable, String coreDwcTerm) {
    return String.format(
        "%s_ext_%s_%s",
        coreDwcTerm, extensionTable.getLeafNamespace(), extensionTable.getHiveTableName());
  }
}
