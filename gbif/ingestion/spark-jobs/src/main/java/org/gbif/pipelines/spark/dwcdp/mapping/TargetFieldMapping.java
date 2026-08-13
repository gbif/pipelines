package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.List;
import java.util.Objects;

/** Describes how one or more path-qualified source fields populate one DwC-A term. */
public record TargetFieldMapping(
    String targetTerm,
    SourceMode sourceMode,
    List<FieldRef> sources,
    ValueAggregation aggregation,
    Origin origin) {

  public enum SourceMode { ONE_OF, ALL_OF }

  /** Whether the producer was deliberately declared or inferred by generic direct-field mapping. */
  public enum Origin { EXPLICIT, INFERRED }

  public TargetFieldMapping {
    Objects.requireNonNull(targetTerm, "targetTerm");
    Objects.requireNonNull(sourceMode, "sourceMode");
    sources = List.copyOf(sources);
    Objects.requireNonNull(aggregation, "aggregation");
    Objects.requireNonNull(origin, "origin");
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("Target field requires at least one source");
    }
  }

  public static TargetFieldMapping oneOf(
      String targetTerm, ValueAggregation aggregation, FieldRef... sources) {
    return new TargetFieldMapping(
        targetTerm, SourceMode.ONE_OF, List.of(sources), aggregation, Origin.EXPLICIT);
  }

  public static TargetFieldMapping inferredOneOf(
      String targetTerm, ValueAggregation aggregation, FieldRef... sources) {
    return new TargetFieldMapping(
        targetTerm, SourceMode.ONE_OF, List.of(sources), aggregation, Origin.INFERRED);
  }

  public static TargetFieldMapping allOf(
      String targetTerm, ValueAggregation aggregation, FieldRef... sources) {
    return new TargetFieldMapping(
        targetTerm, SourceMode.ALL_OF, List.of(sources), aggregation, Origin.EXPLICIT);
  }
}
