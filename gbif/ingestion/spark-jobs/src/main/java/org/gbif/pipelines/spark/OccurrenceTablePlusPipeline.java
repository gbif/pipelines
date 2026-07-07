package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.*;
import static org.gbif.pipelines.spark.ArgsConstants.CONFIG_PATH_ARG;
import static org.gbif.pipelines.spark.ArgsConstants.SPARK_MASTER_ARG;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import org.apache.spark.sql.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

public class OccurrenceTablePlusPipeline {

  private static final String COL_UUID = "7ddf754f-d193-4cc9-b351-99906754a03b";
  private static final String GBIF_UUID = "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c";
  private static final String ZA_UUID = "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c";

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = CONFIG_PATH_ARG, description = "Path to YAML configuration file")
    @SuppressWarnings("FieldMayBeFinal")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = SPARK_MASTER_ARG,
        description = "Spark master - there for local dev only",
        required = false)
    @SuppressWarnings("unused")
    private String master;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    @SuppressWarnings("unused")
    private boolean help;

    @Parameter(names = "--sourceDB", required = true)
    private String sourceDB;

    @Parameter(names = "--targetDB", required = true)
    private String targetDB;
  }

  public static void main(String[] argsv) throws IOException {

    OccurrenceTablePlusPipeline.Args args = new OccurrenceTablePlusPipeline.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    if (config == null) {
      System.err.println(
          "Invalid configuration file. Please provide a valid YAML configuration file.");
      throw new IllegalArgumentException(
          "Invalid configuration file. Missing indexConfig or elastic configuration.");
    }

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master,
            "OccurrenceTablePlusBuild",
            config,
            ReducePartitioningPipeline::configSparkSession);

    // select the occurrence table into a dataframe
    Dataset<Row> occDf = spark.sql(String.format("SELECT * FROM %s.occurrence", args.sourceDB));

    // add new columns
    Dataset<Row> occDfPlus = addNestedStructs(occDf);

    // write iceberg table
    occDfPlus.write().mode(SaveMode.Append).insertInto(args.targetDB + "." + "occurrence_plus");

    spark.close();
  }

  private static final String[] FIELDS =
      new String[] {
        "taxonkey",
        "scientificname",
        "acceptedtaxonkey",
        "acceptednameusageid",
        "acceptedscientificname",
        "genericname",
        "specificepithet",
        "infraspecificepithet",
        "taxonrank",
        "kingdomkey",
        "phylumkey",
        "classkey",
        "orderkey",
        "superfamilykey",
        "familykey",
        "subfamilykey",
        "genuskey",
        "tribekey",
        "subtribekey",
        "specieskey",
        "kingdom",
        "phylum",
        "class",
        "order",
        "superfamily",
        "family",
        "subfamily",
        "genus",
        "tribe",
        "subtribe",
        "species",
        "iucnredlistcategory"
      };

  public static Dataset<Row> addNestedStructs(Dataset<Row> df) {

    Column colClassification = buildStructFromMap(COL_UUID);
    Column gbifClassification = buildStructFromMap(GBIF_UUID);
    Column zaClassification = buildStructFromMap(ZA_UUID);

    return df.withColumn("col_classification", colClassification)
        .withColumn("gbif_classification", gbifClassification)
        .withColumn("za_classification", zaClassification);
  }

  /** Extracts: classificationdetails[uuid][field] -> struct(field1, field2, ...) */
  private static Column buildStructFromMap(String uuid) {

    Column innerMap = element_at(col("classificationdetails"), lit(uuid));

    // plus taxonkeys, taxonomic issues and taxonomic status
    Column[] structFields = new Column[FIELDS.length + 1 + 1 + 1];

    for (int i = 0; i < FIELDS.length; i++) {
      String field = FIELDS[i];
      structFields[i] = element_at(innerMap, lit(field)).alias(field);
    }

    Column clMap = element_at(col("classifications"), lit(uuid));
    structFields[FIELDS.length] = clMap.alias("taxonkeys");

    Column tiMap = element_at(col("taxonomicissue"), lit(uuid));
    structFields[FIELDS.length + 1] = tiMap.alias("issues");

    Column tsMap = element_at(col("taxonomicstatuses"), lit(uuid));
    structFields[FIELDS.length + 2] = tsMap.alias("taxonomicstatus");

    return struct(structFields);
  }
}
