package org.gbif.pipelines.spark;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.Test;

public class FullRecordsTableBuildPipelineTest {

  @Test
  public void datasetsAreBatchedByRecordCount() {
    Map<String, Integer> attempts = Map.of("a", 1, "b", 2, "c", 3, "d", 4, "e", 5);
    Map<String, Long> counts = Map.of("a", 40L, "b", 50L, "c", 30L, "e", 10L);

    List<Map<String, Integer>> batches =
        FullRecordsTableBuildPipeline.batches(attempts, counts, 100);

    // d has no records and joins the batch being built
    assertEquals(List.of(Map.of("a", 1, "b", 2), Map.of("c", 3, "d", 4, "e", 5)), batches);
  }

  @Test
  public void largeDatasetsAreBatchesOfTheirOwn() {
    Map<String, Integer> attempts = Map.of("a", 1, "b", 1, "c", 1);
    Map<String, Long> counts = Map.of("a", 10L, "b", 500L, "c", 10L);

    List<Map<String, Integer>> batches =
        FullRecordsTableBuildPipeline.batches(attempts, counts, 100);

    assertEquals(List.of(Map.of("a", 1), Map.of("b", 1), Map.of("c", 1)), batches);
  }

  @Test
  public void datasetKeysAreReadFromThePaths() {
    assertEquals(
        Map.of("d1", "hdfs://ha-nn/data/ingest/d1/3/json/"),
        FullRecordsTableBuildPipeline.datasetPaths(List.of("hdfs://ha-nn/data/ingest/d1/3/json/")));
  }
}
