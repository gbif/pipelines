package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.Objects;

/** Explicit policy for a relation that may yield more than one target row. */
public sealed interface CardinalityStrategy
    permits CardinalityStrategy.FanOut,
        CardinalityStrategy.ExactlyOne,
        CardinalityStrategy.Select,
        CardinalityStrategy.Combine {

  record FanOut() implements CardinalityStrategy {}

  record ExactlyOne() implements CardinalityStrategy {}

  record Select(String selector) implements CardinalityStrategy {
    public Select {
      Objects.requireNonNull(selector, "selector");
    }
  }

  record Combine(ValueAggregation aggregation) implements CardinalityStrategy {
    public Combine {
      Objects.requireNonNull(aggregation, "aggregation");
    }
  }

  static CardinalityStrategy fanOut() {
    return new FanOut();
  }

  static CardinalityStrategy exactlyOne() {
    return new ExactlyOne();
  }

  static CardinalityStrategy select(String selector) {
    return new Select(selector);
  }

  static CardinalityStrategy combine(ValueAggregation aggregation) {
    return new Combine(aggregation);
  }
}
