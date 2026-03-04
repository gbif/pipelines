package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.*;
import static org.gbif.terms.utils.TermUtils.INTERPRETED_HUMBOLDT_TERMS;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.download.hive.EventHDFSTableDefinition;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.occurrence.download.hive.HiveDataTypes;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class TableUtil {

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

  public static String getCreateTableSQL(DatasetType datasetType, String prefix, String tableName) {
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
        prefix, tableName, getFieldDefinitions(datasetType));
  }

  static String getFieldDefinitions(DatasetType datasetType) {

    if (datasetType == DatasetType.OCCURRENCE) {
      return OccurrenceHDFSTableDefinition.definition().stream()
          .map(field -> field.getHiveField() + " " + field.getHiveDataType())
          .collect(Collectors.joining(", \n"));
    }
    if (datasetType == DatasetType.SAMPLING_EVENT) {
      return EventHDFSTableDefinition.definition().stream()
          .map(field -> field.getHiveField() + " " + field.getHiveDataType())
          .collect(Collectors.joining(", \n"));
    }
    throw new IllegalArgumentException("Unsupported dataset type: " + datasetType);
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

  /**
   * Creates a map of HDFS columns with their corresponding Iceberg column names and select
   * statements.
   *
   * @param hdfs parquet dataset
   * @return map of HDFS columns
   */
  @NotNull
  public static Map<String, TableBuild.HdfsColumn> getHdfsColumns(Dataset<Row> hdfs) {

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

  public static String getCreateTableSQL(DatasetType datasetType, String tableName) {
    return getCreateTableSQL(datasetType, "", tableName);
  }

  public static String getCreateMultimediaTableSQL(String coreDwcTerm) {
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
        coreDwcTerm);
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

  public static void insertOverwriteHumboldtTableFromTemp(
      SparkSession spark, String sourceTableName, String humboldtTable) {
    spark
        .table(sourceTableName)
        .select(
            col("gbifid"),
            from_json(
                    expr("cast(base64_decode(extHumboldt) as string)"),
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
                    expr("cast(base64_decode(extMultimedia) as string)"),
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
                     (gbifid STRING, %s, datasetkey STRING)
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
        tableName, selectTerms);
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

  public static String createVerbatimExtensionTableSQL(
      ExtensionTable extensionTable, String coreDwcTerm) {

    // generate field list
    String fieldList =
        extensionTable.getSchema().getFields().stream()
            .map(f -> f.name() + " STRING")
            .collect(Collectors.joining(",\n "));

    return String.format(
        """
                    CREATE TABLE IF NOT EXISTS %s
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
                    'write.merge.isolation-level' = 'snapshot'
                )
                """,
        verbatimExtensionTableName(extensionTable, coreDwcTerm), fieldList);
  }

  public static String verbatimExtensionTableName(
      ExtensionTable extensionTable, String coreDwcTerm) {
    return String.format(
        "%s_ext_%s_%s",
        coreDwcTerm, extensionTable.getLeafNamespace(), extensionTable.getHiveTableName());
  }
}
