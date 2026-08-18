package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Describes how one or more path-qualified source fields populate one DwC-A term. */
public record TargetFieldMapping(
    String targetTerm,
    SourceMode sourceMode,
    List<FieldRef> sources,
    ValueAggregation aggregation,
    Origin origin,
    Optional<FieldRef> contributionIdentity,
    Optional<FieldRef> orderBy) {

  public enum SourceMode {
    ONE_OF,
    ALL_OF
  }

  /** Whether the producer was deliberately declared or inferred by generic direct-field mapping. */
  public enum Origin {
    EXPLICIT,
    INFERRED
  }

  public TargetFieldMapping {
    Objects.requireNonNull(targetTerm, "targetTerm");
    Objects.requireNonNull(sourceMode, "sourceMode");
    sources = List.copyOf(sources);
    Objects.requireNonNull(aggregation, "aggregation");
    Objects.requireNonNull(origin, "origin");
    contributionIdentity = contributionIdentity == null ? Optional.empty() : contributionIdentity;
    orderBy = orderBy == null ? Optional.empty() : orderBy;
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("Target field requires at least one source");
    }
  }

  public static TargetFieldMapping oneOf(
      String targetTerm, ValueAggregation aggregation, FieldRef... sources) {
    return new TargetFieldMapping(
        targetTerm,
        SourceMode.ONE_OF,
        List.of(sources),
        aggregation,
        Origin.EXPLICIT,
        Optional.empty(),
        Optional.empty());
  }

  public static TargetFieldMapping inferredOneOf(
      String targetTerm, ValueAggregation aggregation, FieldRef... sources) {
    return new TargetFieldMapping(
        targetTerm,
        SourceMode.ONE_OF,
        List.of(sources),
        aggregation,
        Origin.INFERRED,
        Optional.empty(),
        Optional.empty());
  }

  public static TargetFieldMapping allOf(
      String targetTerm, ValueAggregation aggregation, FieldRef... sources) {
    return new TargetFieldMapping(
        targetTerm,
        SourceMode.ALL_OF,
        List.of(sources),
        aggregation,
        Origin.EXPLICIT,
        Optional.empty(),
        Optional.empty());
  }

  /**
   * Identity of the logical contribution, used to deduplicate the same linked record across paths.
   */
  public TargetFieldMapping contributionIdentity(FieldRef field) {
    return new TargetFieldMapping(
        targetTerm, sourceMode, sources, aggregation, origin, Optional.of(field), orderBy);
  }

  /**
   * Field used to deterministically order this producer's contributions before target aggregation.
   */
  public TargetFieldMapping orderBy(FieldRef field) {
    return new TargetFieldMapping(
        targetTerm,
        sourceMode,
        sources,
        aggregation,
        origin,
        contributionIdentity,
        Optional.of(field));
  }
}
