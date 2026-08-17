package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.CardinalityStrategy;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationStep;
/** Funnel-style metrics emitted while executing one relation step. */
public record RelationExecutionMetrics(
    String sourceResource,
    String targetResource,
    String cardinality,
    String requirement,
    boolean filtered,
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
      String sourceResource,
      String targetResource,
      RelationStep step,
      long inputRows) {
    return new RelationExecutionMetrics(
        sourceResource,
        targetResource,
        cardinalityName(step),
        step.requirement().name(),
        step.filter().isPresent(),
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

  static String cardinalityName(RelationStep step) {
    CardinalityStrategy strategy =
        step.cardinalityStrategy().orElseGet(CardinalityStrategy::exactlyOne);
    if (strategy instanceof CardinalityStrategy.ExactlyOne) {
      return "EXACTLY_ONE";
    }
    if (strategy instanceof CardinalityStrategy.FanOut) {
      return "FAN_OUT";
    }
    if (strategy instanceof CardinalityStrategy.Select select) {
      return "SELECT(" + select.selector() + ")";
    }
    if (strategy instanceof CardinalityStrategy.Combine) {
      return "COMBINE";
    }
    return strategy.getClass().getSimpleName();
  }
}
