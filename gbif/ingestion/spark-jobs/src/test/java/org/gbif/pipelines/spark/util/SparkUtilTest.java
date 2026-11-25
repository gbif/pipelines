package org.gbif.pipelines.spark.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.spark.pojo.Occurrence;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkUtilTest {

  private static SparkSession spark;

  @BeforeClass
  public static void setUp() {
    spark = SparkTestSession.create();
  }

  @AfterClass
  public static void tearDown() {
    spark.stop();
  }

  /** Simple records written before the DNA taxonomy, e.g. of a dataset not interpreted again. */
  @Test
  public void readParquetAsReadsSimpleRecordsWithoutTheNewFields() throws Exception {
    String path =
        new File(Files.createTempDirectory("simple-occurrence").toFile(), "simple").getPath();
    Occurrence occurrence = Occurrence.builder().id("1").taxon("{}").dnaTaxon("{}").build();
    spark
        .createDataset(List.of(occurrence), Encoders.bean(Occurrence.class))
        .toDF()
        .drop("dnaTaxon")
        .write()
        .parquet(path);

    // reading them with the schema of the files fails
    assertThrows(
        AnalysisException.class,
        () -> spark.read().parquet(path).as(Encoders.bean(Occurrence.class)).collectAsList());

    List<Occurrence> read = SparkUtil.readParquetAs(spark, path, Occurrence.class).collectAsList();

    assertEquals(1, read.size());
    assertEquals("1", read.get(0).getId());
    assertEquals("{}", read.get(0).getTaxon());
    assertNull(read.get(0).getDnaTaxon());
  }

  /** JSON records written before the DNA taxonomy, indexed again without interpreting them. */
  @Test
  public void readParquetAsReadsJsonRecordsWithoutTheNewFields() throws Exception {
    String path = new File(Files.createTempDirectory("occurrence-json").toFile(), "json").getPath();
    OccurrenceJsonRecord record = new OccurrenceJsonRecord();
    record.setGbifId(1L);
    record.setDatasetKey("dataset");
    spark
        .createDataset(List.of(record), Encoders.bean(OccurrenceJsonRecord.class))
        .toDF()
        .drop("dnaChecklistKey", "dnaClassifications")
        .write()
        .parquet(path);

    List<OccurrenceJsonRecord> read =
        SparkUtil.readParquetAs(spark, path, OccurrenceJsonRecord.class).collectAsList();

    assertEquals(1, read.size());
    assertEquals(1L, (long) read.get(0).getGbifId());
    assertEquals("dataset", read.get(0).getDatasetKey());
    assertNull(read.get(0).getDnaChecklistKey());
    assertNull(read.get(0).getDnaClassifications());
  }
}
