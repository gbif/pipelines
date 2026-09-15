package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Describes how one or more path-qualified source fields populate one DwC-A term. */
public record TargetFieldMapping(
    String targetTerm,
    TargetValue value,
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
    Objects.requireNonNull(value, "value");
    Objects.requireNonNull(origin, "origin");
    contributionIdentity = contributionIdentity == null ? Optional.empty() : contributionIdentity;
    orderBy = orderBy == null ? Optional.empty() : orderBy;
    if (value instanceof TargetValue.Expression
        && (contributionIdentity.isPresent() || orderBy.isPresent())) {
      throw new IllegalArgumentException(
          "Expression target values do not support contribution identity or ordering");
    }
  }

  public static TargetFieldMapping oneOf(
      String targetTerm, ValueAggregation aggregation, FieldRef... sources) {
    return aggregated(
        targetTerm, SourceMode.ONE_OF, aggregation, Origin.EXPLICIT, List.of(sources));
  }

  public static TargetFieldMapping inferredOneOf(
      String targetTerm, ValueAggregation aggregation, FieldRef... sources) {
    return aggregated(
        targetTerm, SourceMode.ONE_OF, aggregation, Origin.INFERRED, List.of(sources));
  }

  public static TargetFieldMapping allOf(
      String targetTerm, ValueAggregation aggregation, FieldRef... sources) {
    return aggregated(
        targetTerm, SourceMode.ALL_OF, aggregation, Origin.EXPLICIT, List.of(sources));
  }

  public static TargetFieldMapping expression(String targetTerm, ValueExpression expression) {
    return new TargetFieldMapping(
        targetTerm,
        new TargetValue.Expression(expression),
        Origin.EXPLICIT,
        Optional.empty(),
        Optional.empty());
  }

  public static TargetFieldMapping inferredExpression(
      String targetTerm, ValueExpression expression) {
    return new TargetFieldMapping(
        targetTerm,
        new TargetValue.Expression(expression),
        Origin.INFERRED,
        Optional.empty(),
        Optional.empty());
  }

  private static TargetFieldMapping aggregated(
      String targetTerm,
      SourceMode sourceMode,
      ValueAggregation aggregation,
      Origin origin,
      List<FieldRef> sources) {
    return new TargetFieldMapping(
        targetTerm,
        new TargetValue.Aggregated(sourceMode, sources, aggregation),
        origin,
        Optional.empty(),
        Optional.empty());
  }

  public List<FieldRef> sources() {
    return value.sources();
  }

  public boolean expressionValue() {
    return value instanceof TargetValue.Expression;
  }

  public SourceMode sourceMode() {
    return aggregatedValue().sourceMode();
  }

  public ValueAggregation aggregation() {
    return aggregatedValue().aggregation();
  }

  public ValueExpression expression() {
    if (value instanceof TargetValue.Expression expressionValue) {
      return expressionValue.expression();
    }
    throw new IllegalStateException("Target field is not expression-backed: " + targetTerm);
  }

  /**
   * Identity of the logical contribution, used to deduplicate the same linked record across paths.
   */
  public TargetFieldMapping contributionIdentity(FieldRef field) {
    aggregatedValue();
    return new TargetFieldMapping(targetTerm, value, origin, Optional.of(field), orderBy);
  }

  /**
   * Field used to deterministically order this producer's contributions before target aggregation.
   */
  public TargetFieldMapping orderBy(FieldRef field) {
    aggregatedValue();
    return new TargetFieldMapping(
        targetTerm, value, origin, contributionIdentity, Optional.of(field));
  }

  private TargetValue.Aggregated aggregatedValue() {
    if (value instanceof TargetValue.Aggregated aggregated) {
      return aggregated;
    }
    throw new IllegalStateException("Target field is not aggregation-backed: " + targetTerm);
  }
}
