package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.Directories.IDENTIFIERS;
import static org.gbif.pipelines.spark.IdentifiersPipeline.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.OptionalInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CompareIdentifiersTest {

  private static SparkSession spark;
  private static FileSystem fs;

  @BeforeClass
  public static void setUp() throws Exception {
    spark = SparkTestSession.createBuilder().appName("test").getOrCreate();
    fs = FileSystem.getLocal(new Configuration());
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  private static Dataset<IdentifierRecord> identifiers(String... gbifIds) {
    return spark.createDataset(
        Arrays.stream(gbifIds)
            .map(
                gbifId ->
                    IdentifierRecord.newBuilder()
                        .setId("id-" + gbifId)
                        .setInternalId(gbifId)
                        .build())
            .toList(),
        Encoders.bean(IdentifierRecord.class));
  }

  @Test
  public void testNewAndRemoved() throws Exception {

    String root = Files.createTempDirectory("compare-identifiers").toString();

    // attempt 1 & 2 completed, attempt 3 has no identifiers, attempt 5 is above the current
    identifiers("1").write().parquet(root + "/1/" + IDENTIFIERS);
    identifiers("1", "2", "3").write().parquet(root + "/2/" + IDENTIFIERS);
    fs.mkdirs(new Path(root + "/3"));
    identifiers("9").write().parquet(root + "/5/" + IDENTIFIERS);

    assertEquals(OptionalInt.of(2), findPreviousAttempt(fs, root, 4));

    // 2 & 3 retained, 1 removed, 4 new with a GBIF id, plus one new without a GBIF id
    Dataset<IdentifierRecord> current = identifiers("2", "3", "4", null);

    Map<String, Long> metrics = compareWithPreviousAttempt(spark, fs, root, 4, current);

    assertEquals(Long.valueOf(2), metrics.get(PREVIOUS_ATTEMPT));
    assertEquals(Long.valueOf(3), metrics.get(PREVIOUS_IDENTIFIERS_COUNT));
    assertEquals(Long.valueOf(4), metrics.get(CURRENT_IDENTIFIERS_COUNT));
    assertEquals(Long.valueOf(2), metrics.get(NEW_IDENTIFIERS_COUNT));
    assertEquals(Long.valueOf(1), metrics.get(REMOVED_IDENTIFIERS_COUNT));
  }

  @Test
  public void testNoPreviousAttempt() throws Exception {

    String root = Files.createTempDirectory("compare-identifiers").toString();

    Map<String, Long> metrics =
        compareWithPreviousAttempt(spark, fs, root, 1, identifiers("1", "2"));

    assertEquals(Long.valueOf(2), metrics.get(CURRENT_IDENTIFIERS_COUNT));
    assertFalse(metrics.containsKey(NEW_IDENTIFIERS_COUNT));
    assertFalse(metrics.containsKey(REMOVED_IDENTIFIERS_COUNT));
  }
}
