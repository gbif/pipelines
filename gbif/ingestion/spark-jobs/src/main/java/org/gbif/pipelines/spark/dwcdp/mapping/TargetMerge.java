package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Objects;

/** Explicit permission to merge independent producers into one target term. */
public record TargetMerge(String targetTerm, ValueAggregation aggregation) {
  public TargetMerge {
    Objects.requireNonNull(targetTerm, "targetTerm");
    Objects.requireNonNull(aggregation, "aggregation");
  }
}
