package org.gbif.pipelines.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class FullIndexBuildPipelineTest {

  private static SparkSession spark;

  @BeforeClass
  public static void setUp() {
    spark = SparkTestSession.create();
  }

  @AfterClass
  public static void tearDown() {
    spark.stop();
  }

  /**
   * With mergeSchema, the json of an attempt written before a column existed must not drop it for
   * the attempts written after, e.g. dnaChecklistKey.
   */
  @Test
  public void readJsonParquetMergesTheSchemas() throws Exception {
    File dir = Files.createTempDirectory("full-index-build").toFile();
    // the old attempt sorts first, so its schema would be the one taken without merging, as for a
    // dataset key lower than the one of the new attempt
    String oldAttempt = new File(dir, "a-old-dataset").getAbsolutePath();
    String newAttempt = new File(dir, "b-new-dataset").getAbsolutePath();

    StructType oldSchema =
        new StructType().add("gbifId", DataTypes.LongType).add("datasetkey", DataTypes.StringType);
    StructType newSchema =
        oldSchema.add("dnaChecklistKey", DataTypes.createArrayType(DataTypes.StringType));
    spark
        .createDataFrame(List.of(RowFactory.create(1L, "old-dataset")), oldSchema)
        .write()
        .parquet(oldAttempt);
    spark
        .createDataFrame(
            List.of(RowFactory.create(2L, "new-dataset", List.of("col-checklist"))), newSchema)
        .write()
        .parquet(newAttempt);

    Dataset<Row> json =
        FullIndexBuildPipeline.readJsonParquet(spark, List.of(oldAttempt, newAttempt), true);

    assertTrue(List.of(json.schema().fieldNames()).contains("dnaChecklistKey"));
    Map<Long, Row> rows =
        json.collectAsList().stream()
            .collect(Collectors.toMap(r -> r.<Long>getAs("gbifId"), r -> r));
    assertEquals(
        List.of("col-checklist"), rows.get(2L).getList(rows.get(2L).fieldIndex("dnaChecklistKey")));
    assertNull(rows.get(1L).getAs("dnaChecklistKey"));
  }
}
