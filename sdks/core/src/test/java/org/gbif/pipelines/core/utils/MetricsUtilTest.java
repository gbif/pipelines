package org.gbif.pipelines.core.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetricsUtilTest {

  @TempDir Path tempDir;

  @Test
  void writeThenRead_mixedMagnitudes_roundTripsAsLong() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String fileName = tempDir.resolve("archive-to-verbatim.yml").toString();

    // smallCount fits in an int -> SnakeYAML will box it as Integer on load.
    // largeCount doesn't fit in an int -> SnakeYAML will box it as Long on load.
    Map<String, Long> written = new HashMap<>();
    written.put("smallCount", 42L);
    written.put("largeCount", 5_000_000_000L);

    MetricsUtil.writeMetricsYaml(fs, written, fileName);

    Map<String, Long> read = MetricsUtil.readMetricsYaml(fs, fileName);

    // Both come back as genuine Long values, regardless of on-disk magnitude.
    assertEquals(42L, read.getOrDefault("smallCount", 0L));
    assertEquals(5_000_000_000L, read.getOrDefault("largeCount", 0L));
  }

  @Test
  void readMetricsYaml_missingFile_returnsEmptyMap() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String fileName = tempDir.resolve("does-not-exist.yml").toString();

    Map<String, Long> read = MetricsUtil.readMetricsYaml(fs, fileName);

    assertEquals(Map.of(), read);
  }

  @Test
  void readMetricsYaml_beforeFix_demonstratesTheBug() throws IOException {
    // Historical regression check: this is the exact failure mode from production.
    // Without the Number-normalization fix, this line would throw:
    //   ClassCastException: class java.lang.Integer cannot be cast to class java.lang.Long
    // at the call site (e.g. DwcDpToVerbatimCallback.readMetric), not inside readMetricsYaml
    // itself, because the unchecked generic cast happens at the *caller's* checkcast.
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String fileName = tempDir.resolve("small-only.yml").toString();

    Map<String, Long> written = Map.of("occCount", 42L);
    MetricsUtil.writeMetricsYaml(fs, written, fileName);

    Map<String, Long> read = MetricsUtil.readMetricsYaml(fs, fileName);

    // With the fix in place this succeeds cleanly.
    // (Kept as a positive assertion rather than assertThrows, since the fix
    // is what we're locking in — this test documents *why* it matters.)
    assertEquals(42L, read.getOrDefault("occCount", 0L));
  }
}
