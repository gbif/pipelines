package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.List;
import java.util.Objects;

/** Describes how one or more candidate source fields populate one output field. */
public record TargetMapping(
    String targetField,
    SourceMode sourceMode,
    List<FieldSource> sources,
    ValueAggregation aggregation) {

  public enum SourceMode {
    ONE_OF,
    ALL_OF
  }

  public TargetMapping {
    Objects.requireNonNull(targetField, "targetField");
    Objects.requireNonNull(sourceMode, "sourceMode");
    sources = List.copyOf(sources);
    Objects.requireNonNull(aggregation, "aggregation");
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("Target mapping requires at least one source");
    }
  }

  public static TargetMapping oneOf(
      String targetField, ValueAggregation aggregation, FieldSource... sources) {
    return new TargetMapping(targetField, SourceMode.ONE_OF, List.of(sources), aggregation);
  }

  public static TargetMapping allOf(
      String targetField, ValueAggregation aggregation, FieldSource... sources) {
    return new TargetMapping(targetField, SourceMode.ALL_OF, List.of(sources), aggregation);
  }
}
