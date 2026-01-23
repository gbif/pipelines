package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DUPLICATE_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.UNIQUE_IDS_COUNT;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ValidateIdentifiersTest {

  private static SparkSession spark;

  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate();
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void testAllUnique() {

    Map<String, Long> metrics = new HashMap<>();

    // Example input dataset
    List<ExtendedRecord> records =
        Arrays.asList(
            ExtendedRecord.newBuilder().setId("1").build(),
            ExtendedRecord.newBuilder().setId("2").build(),
            ExtendedRecord.newBuilder().setId("3").build());

    // Create Dataset<Person>
    Dataset<ExtendedRecord> dataset =
        spark.createDataset(records, Encoders.bean(ExtendedRecord.class));

    ValidateIdentifiers.validateIdentifiers(dataset, metrics);

    assert metrics.get(UNIQUE_IDS_COUNT + "Attempted") == 3;
  }

  @Test
  public void testDuplicates() {

    Map<String, Long> metrics = new HashMap<>();
    // Example input dataset
    List<ExtendedRecord> records =
        Arrays.asList(
            ExtendedRecord.newBuilder().setId("1").build(),
            ExtendedRecord.newBuilder().setId("2").build(),
            ExtendedRecord.newBuilder().setId("3").build(),
            ExtendedRecord.newBuilder().setId("4").build(),
            ExtendedRecord.newBuilder().setId("4").build(),
            ExtendedRecord.newBuilder().setId("4").build());

    // Create Dataset<Person>
    Dataset<ExtendedRecord> dataset =
        spark.createDataset(records, Encoders.bean(ExtendedRecord.class));

    ValidateIdentifiers.validateIdentifiers(dataset, metrics);

    assert metrics.get(UNIQUE_IDS_COUNT + "Attempted") == 3;
    assert metrics.get(DUPLICATE_IDS_COUNT + "Attempted") == 3;
  }
}
