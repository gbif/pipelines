package org.gbif.pipelines.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.TableBuildConfig;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * The full rebuild reads parquet written by different pipeline versions. Files written before a
 * field was added (e.g. environments) must not cause newer files to lose it, regardless of which
 * file Spark sees first.
 */
public class FullTableBuildPipelineTest {

  private static final String GBIF_BACKBONE = "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c";

  private static SparkSession spark;

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @BeforeClass
  public static void setUp() {
    spark = SparkTestSession.create();
  }

  @AfterClass
  public static void tearDown() {
    spark.stop();
  }

  private static PipelinesConfig config() {
    TableBuildConfig tableBuildConfig = new TableBuildConfig();
    tableBuildConfig.setClassifications(Map.of(GBIF_BACKBONE, "gbif"));
    PipelinesConfig config = new PipelinesConfig();
    config.setTableBuildConfig(tableBuildConfig);
    return config;
  }

  @Test
  public void expectedSchemaIncludesEnvironments() {
    StructType schema =
        FullTableBuildPipeline.expectedHdfsSchema(spark, config(), DatasetType.OCCURRENCE);

    assertTrue(Arrays.asList(schema.fieldNames()).contains("environments"));
    StructType gbifClassification = (StructType) schema.apply("gbif_classification").dataType();
    assertTrue(Arrays.asList(gbifClassification.fieldNames()).contains("environments"));
  }

  @Test
  public void oldParquetWithoutEnvironmentsDoesNotDropItFromNewParquet() throws Exception {
    StructType full =
        FullTableBuildPipeline.expectedHdfsSchema(spark, config(), DatasetType.OCCURRENCE);
    StructField gbifid = full.apply("gbifid");
    StructField environments = full.apply("environments");
    StructType classification = (StructType) full.apply("gbif_classification").dataType();
    StructField taxonKey = classification.apply("taxonkey");
    StructField nestedEnvironments = classification.apply("environments");

    // written before environments existed, at the top level and in the classification struct
    StructType oldClassification = new StructType(new StructField[] {taxonKey});
    StructType oldSchema =
        new StructType(
            new StructField[] {
              gbifid,
              new StructField("gbif_classification", oldClassification, true, Metadata.empty())
            });
    File oldDir = tmp.newFolder("old");
    write(oldSchema, RowFactory.create("1", RowFactory.create("k1")), oldDir);

    // written by the current pipeline
    StructType newClassification = new StructType(new StructField[] {taxonKey, nestedEnvironments});
    StructType newSchema =
        new StructType(
            new StructField[] {
              gbifid,
              environments,
              new StructField("gbif_classification", newClassification, true, Metadata.empty())
            });
    File newDir = tmp.newFolder("new");
    write(
        newSchema,
        RowFactory.create(
            "2", List.of("marine"), RowFactory.create("k2", List.of("marine", "brackish"))),
        newDir);

    // old file first: the order in which schema inference would drop environments
    List<String> paths = new ArrayList<>();
    paths.addAll(parquetFiles(oldDir));
    paths.addAll(parquetFiles(newDir));

    Dataset<Row> hdfs = FullTableBuildPipeline.readHdfsParquet(spark, full, paths);
    hdfs.createOrReplaceTempView("hdfs_mixed");

    List<Row> rows =
        spark
            .sql(
                "SELECT gbifid, environments, gbif_classification.taxonkey,"
                    + " gbif_classification.environments FROM hdfs_mixed ORDER BY gbifid")
            .collectAsList();

    assertEquals(2, rows.size());

    Row old = rows.get(0);
    assertEquals("1", old.getString(0));
    assertNull(old.get(1));
    assertEquals("k1", old.getString(2));
    assertNull(old.get(3));

    Row current = rows.get(1);
    assertEquals("2", current.getString(0));
    assertEquals(List.of("marine"), current.getList(1));
    assertEquals("k2", current.getString(2));
    assertEquals(List.of("marine", "brackish"), current.getList(3));
  }

  private static void write(StructType schema, Row row, File dir) {
    spark
        .createDataFrame(Collections.singletonList(row), schema)
        .coalesce(1)
        .write()
        .mode("overwrite")
        .parquet(dir.getAbsolutePath());
  }

  private static List<String> parquetFiles(File dir) {
    List<String> files = new ArrayList<>();
    for (File f : dir.listFiles()) {
      if (f.getName().endsWith(".parquet")) {
        files.add(f.getAbsolutePath());
      }
    }
    return files;
  }
}
