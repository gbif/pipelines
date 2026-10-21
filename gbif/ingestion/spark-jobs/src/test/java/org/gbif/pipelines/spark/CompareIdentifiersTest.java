package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.Directories.IDENTIFIERS;
import static org.gbif.pipelines.spark.Directories.OCCURRENCE_JSON;
import static org.gbif.pipelines.spark.IdentifiersPipeline.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.OptionalInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompareIdentifiersTest {

  private static SparkSession spark;
  private static FileSystem fs;

  @BeforeAll
  static void setUp() throws Exception {
    spark = SparkTestSession.createBuilder().appName("test").getOrCreate();
    fs = FileSystem.getLocal(new Configuration());
  }

  @AfterAll
  static void tearDown() {
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

  /** Marks the interpretation of an attempt as completed. */
  private static void markInterpreted(Path root, int attempt) throws IOException {
    Path dir = root.resolve(attempt + "/" + OCCURRENCE_JSON);
    Files.createDirectories(dir);
    Files.createFile(dir.resolve("_SUCCESS"));
  }

  @Test
  void testNewAndRemoved(@TempDir Path dir) throws Exception {
    String root = dir.toString();

    // attempt 1 & 2 completed, attempt 3 has no identifiers, attempt 5 is above the current
    identifiers("1").write().parquet(root + "/1/" + IDENTIFIERS);
    markInterpreted(dir, 1);
    identifiers("1", "2", "3").write().parquet(root + "/2/" + IDENTIFIERS);
    markInterpreted(dir, 2);
    Files.createDirectories(dir.resolve("3"));
    identifiers("9").write().parquet(root + "/5/" + IDENTIFIERS);
    markInterpreted(dir, 5);

    assertEquals(OptionalInt.of(2), findPreviousAttempt(fs, root, 4));

    // 2 & 3 retained, 1 removed, 4 new with a GBIF id, plus one new without a GBIF id
    Dataset<IdentifierRecord> current = identifiers("2", "3", "4", null);

    Map<String, Long> metrics = compareWithPreviousAttempt(spark, fs, root, 4, current);

    assertEquals(2L, metrics.get(PREVIOUS_ATTEMPT));
    assertEquals(3L, metrics.get(PREVIOUS_IDENTIFIERS_COUNT));
    assertEquals(4L, metrics.get(CURRENT_IDENTIFIERS_COUNT));
    assertEquals(2L, metrics.get(NEW_IDENTIFIERS_COUNT));
    assertEquals(1L, metrics.get(REMOVED_IDENTIFIERS_COUNT));
  }

  @Test
  void testSkipsAttemptWithoutInterpretation(@TempDir Path dir) throws Exception {
    String root = dir.toString();

    identifiers("1").write().parquet(root + "/1/" + IDENTIFIERS);
    markInterpreted(dir, 1);
    // attempt 2 has identifiers but the interpretation never finished
    identifiers("1", "2").write().parquet(root + "/2/" + IDENTIFIERS);

    assertEquals(OptionalInt.of(1), findPreviousAttempt(fs, root, 3));
  }

  @Test
  void testNoPreviousAttempt(@TempDir Path dir) throws Exception {
    String root = dir.toString();

    Map<String, Long> metrics =
        compareWithPreviousAttempt(spark, fs, root, 1, identifiers("1", "2"));

    assertEquals(2L, metrics.get(CURRENT_IDENTIFIERS_COUNT));
    assertFalse(metrics.containsKey(NEW_IDENTIFIERS_COUNT));
    assertFalse(metrics.containsKey(REMOVED_IDENTIFIERS_COUNT));
  }
}
