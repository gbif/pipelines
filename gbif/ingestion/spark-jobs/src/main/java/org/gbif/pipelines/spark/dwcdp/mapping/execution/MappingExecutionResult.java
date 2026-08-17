package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.util.List;

/** Result of executing only the DwC-DP navigation portion of a mapping. */
public record MappingExecutionResult(
    SparkPathResult pathResult,
    List<RelationExecutionMetrics> metrics,
    boolean completePath) {

  public MappingExecutionResult {
    metrics = List.copyOf(metrics);
  }
}
