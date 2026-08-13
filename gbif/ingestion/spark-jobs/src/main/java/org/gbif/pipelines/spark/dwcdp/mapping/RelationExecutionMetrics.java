package org.gbif.pipelines.spark.dwcdp.mapping;

/** Funnel-style metrics emitted while executing one relation step. */
public record RelationExecutionMetrics(
    String sourceResource,
    String targetResource,
    long inputRows,
    long sourceKeyPresentRows,
    long targetRowsBeforeFilter,
    long targetRowsAfterFilter,
    long matchedParentRows,
    long unmatchedParentRows,
    long multipleMatchParentRows,
    long outputRows,
    boolean skipped) {

  static RelationExecutionMetrics skipped(
      String sourceResource, String targetResource, long inputRows) {
    return new RelationExecutionMetrics(
        sourceResource,
        targetResource,
        inputRows,
        0L,
        0L,
        0L,
        0L,
        inputRows,
        0L,
        inputRows,
        true);
  }
}
