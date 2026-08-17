package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.util.ArrayList;
import java.util.List;

/** Mutable execution-local collector used while Spark mapping branches are materialized. */
public final class ExecutionMetricsCollector {
  private final List<MappingBranchExecutionMetrics> branches = new ArrayList<>();

  public void record(String branchName, List<RelationExecutionMetrics> relations) {
    if (!relations.isEmpty()) {
      branches.add(new MappingBranchExecutionMetrics(branchName, relations));
    }
  }

  public List<MappingBranchExecutionMetrics> snapshot() {
    return List.copyOf(branches);
  }
}
