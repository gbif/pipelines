package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.List;
import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

/** Compiler-approved merge of otherwise independent target producers. */
public record CompiledTargetMerge(
    String targetTerm, ValueAggregation aggregation, List<CompiledTargetProducer> producers) {
  public CompiledTargetMerge {
    Objects.requireNonNull(targetTerm, "targetTerm");
    Objects.requireNonNull(aggregation, "aggregation");
    producers = List.copyOf(producers);
    if (producers.isEmpty()) {
      throw new IllegalArgumentException("Compiled target merge requires at least one producer");
    }
  }
}
