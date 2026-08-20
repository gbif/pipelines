package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.util.List;

/** Execution metrics for one named mapping branch and all relation steps it traversed. */
public record MappingBranchExecutionMetrics(
    String branchName, List<RelationExecutionMetrics> relations) {

  public MappingBranchExecutionMetrics {
    relations = List.copyOf(relations);
  }
}
