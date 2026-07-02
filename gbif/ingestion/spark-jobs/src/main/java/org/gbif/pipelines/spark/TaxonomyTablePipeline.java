package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.*;
import static org.gbif.pipelines.spark.ArgsConstants.CONFIG_PATH_ARG;
import static org.gbif.pipelines.spark.ArgsConstants.SPARK_MASTER_ARG;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;
import static org.gbif.pipelines.spark.util.TableUtil.generateTblProperties;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

public class TaxonomyTablePipeline {

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

    TaxonomyTablePipeline.Args args = new TaxonomyTablePipeline.Args();
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
            "TaxonomyTableBuild",
            config,
            ReducePartitioningPipeline::configSparkSession);

    // create the taxonomy table if not exists
    spark.sql(
        String.format(
            """
              DROP TABLE IF EXISTS %s.taxonomy
            """, args.targetDB));

    spark.sql(
        String.format(
            """
              CREATE TABLE %s.taxonomy (
                    taxonkey STRING,
                    scientificname STRING,

                    acceptedtaxonkey STRING,
                    acceptednameusageid STRING,
                    acceptedscientificname STRING,

                    genericname STRING,
                    specificepithet STRING,
                    infraspecificepithet STRING,
                    taxonrank STRING,

                    kingdomkey STRING,
                    phylumkey STRING,
                    classkey STRING,
                    orderkey STRING,
                    superfamilykey STRING,
                    familykey STRING,
                    subfamilykey STRING,
                    genuskey STRING,
                    tribekey STRING,
                    subtribekey STRING,
                    specieskey STRING,

                    kingdom STRING,
                    phylum STRING,
                    class STRING,
                    `order` STRING,
                    superfamily STRING,
                    family STRING,
                    subfamily STRING,
                    genus STRING,
                    tribe STRING,
                    subtribe STRING,
                    species STRING,

                    iucnredlistcategory STRING,
                    taxonkeys ARRAY<STRING>,
                    checklistkey STRING
                )
                USING iceberg
                PARTITIONED BY (checklistkey)
                TBLPROPERTIES (%s)
                """,
            args.targetDB, generateTblProperties(config.getTableBuildConfig())));

    // TODO read MAP gbifid, datasetkey, classificationdetails, taxonomicissue from occurrence
    Dataset<Row> taxonomyContent =
        spark.sql(
            String.format(
                """
                SELECT
                    classifications,
                    classificationdetails
                FROM
                    %s.occurrence
            """,
                args.sourceDB));

    // TODO insert INTO occurrence_taxonomy an entry for each entry in the classificationdetails MAP
    Dataset<Row> occTaxDf = buildOccurrenceTaxonomy(taxonomyContent);

    // write to table
    occTaxDf.write().mode(SaveMode.Append).insertInto(args.targetDB + "." + "taxonomy");

    spark.close();
  }

  static Dataset<Row> buildOccurrenceTaxonomy(Dataset<Row> occ) {
    return occ.withColumn("cld", explode(map_entries(col("classificationdetails"))))
        .select(col("cld.key").alias("checklistkey"), col("cld.value").alias("classification"))
        .withColumn(
            "taxonkeys", element_at(col("classifications"), col("checklistkey"))) // key columns
        .withColumn("taxonkey", col("classification").getItem("taxonkey"))
        .withColumn("scientificname", col("classification").getItem("scientificname"))
        .withColumn("acceptedtaxonkey", col("classification").getItem("acceptedtaxonkey"))
        .withColumn("acceptednameusageid", col("classification").getItem("acceptednameusageid"))
        .withColumn(
            "acceptedscientificname", col("classification").getItem("acceptedscientificname"))
        .withColumn("genericname", col("classification").getItem("genericname"))
        .withColumn("specificepithet", col("classification").getItem("specificepithet"))
        .withColumn("infraspecificepithet", col("classification").getItem("infraspecificepithet"))
        .withColumn("taxonrank", col("classification").getItem("taxonrank"))
        .withColumn("kingdomkey", col("classification").getItem("kingdomkey"))
        .withColumn("phylumkey", col("classification").getItem("phylumkey"))
        .withColumn("classkey", col("classification").getItem("classkey"))
        .withColumn("orderkey", col("classification").getItem("orderkey"))
        .withColumn("superfamilykey", col("classification").getItem("superfamilykey"))
        .withColumn("familykey", col("classification").getItem("familykey"))
        .withColumn("subfamilykey", col("classification").getItem("subfamilykey"))
        .withColumn("genuskey", col("classification").getItem("genuskey"))
        .withColumn("tribekey", col("classification").getItem("tribekey"))
        .withColumn("subtribekey", col("classification").getItem("subtribekey"))
        .withColumn("specieskey", col("classification").getItem("specieskey"))
        .withColumn("kingdom", col("classification").getItem("kingdom"))
        .withColumn("phylum", col("classification").getItem("phylum"))
        .withColumn("class", col("classification").getItem("class"))
        .withColumn("order", col("classification").getItem("order"))
        .withColumn("superfamily", col("classification").getItem("superfamily"))
        .withColumn("family", col("classification").getItem("family"))
        .withColumn("subfamily", col("classification").getItem("subfamily"))
        .withColumn("genus", col("classification").getItem("genus"))
        .withColumn("tribe", col("classification").getItem("tribe"))
        .withColumn("subtribe", col("classification").getItem("subtribe"))
        .withColumn("species", col("classification").getItem("species"))
        .withColumn("iucnredlistcategory", col("classification").getItem("iucnredlistcategory"))
        .select(
            col("taxonkey"),
            col("scientificname"),
            col("acceptedtaxonkey"),
            col("acceptednameusageid"),
            col("acceptedscientificname"),
            col("genericname"),
            col("specificepithet"),
            col("infraspecificepithet"),
            col("taxonrank"),
            col("kingdomkey"),
            col("phylumkey"),
            col("classkey"),
            col("orderkey"),
            col("superfamilykey"),
            col("familykey"),
            col("subfamilykey"),
            col("genuskey"),
            col("tribekey"),
            col("subtribekey"),
            col("specieskey"),
            col("kingdom"),
            col("phylum"),
            col("class"),
            col("order"),
            col("superfamily"),
            col("family"),
            col("subfamily"),
            col("genus"),
            col("tribe"),
            col("subtribe"),
            col("species"),
            col("iucnredlistcategory"),
            col("taxonkeys"),
            col("checklistkey"))
        .dropDuplicates("taxonkey", "checklistkey");
  }
}
