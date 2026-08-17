package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.List;
import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;

/** @deprecated Use {@link CompiledTargetMerge}; target merges are no longer core-specific. */
@Deprecated
public record CompiledCoreTargetMerge(
    String targetTerm, ValueAggregation aggregation, List<CompiledTargetProducer> producers) {
  public CompiledCoreTargetMerge {
    Objects.requireNonNull(targetTerm, "targetTerm");
    Objects.requireNonNull(aggregation, "aggregation");
    producers = List.copyOf(producers);
  }

  public CompiledTargetMerge asTargetMerge() {
    return new CompiledTargetMerge(targetTerm, aggregation, producers);
  }
}
