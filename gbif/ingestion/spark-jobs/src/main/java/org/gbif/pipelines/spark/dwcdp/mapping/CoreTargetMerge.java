package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Objects;

/** @deprecated Use {@link TargetMerge}; target merge semantics are shared by core and extensions. */
@Deprecated
public record CoreTargetMerge(String targetTerm, ValueAggregation aggregation) {
  public CoreTargetMerge {
    Objects.requireNonNull(targetTerm, "targetTerm");
    Objects.requireNonNull(aggregation, "aggregation");
  }

  public TargetMerge asTargetMerge() {
    return new TargetMerge(targetTerm, aggregation);
  }
}
