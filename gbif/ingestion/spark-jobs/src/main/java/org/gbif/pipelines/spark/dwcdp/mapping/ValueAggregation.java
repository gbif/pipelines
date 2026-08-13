package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Objects;

/** How several candidate values are reduced when populating one target field. */
public sealed interface ValueAggregation
    permits ValueAggregation.FirstNonNull,
        ValueAggregation.ExactlyOne,
        ValueAggregation.Delimited,
        ValueAggregation.Named {

  record FirstNonNull() implements ValueAggregation {}

  record ExactlyOne() implements ValueAggregation {}

  record Delimited(String delimiter, boolean distinct) implements ValueAggregation {
    public Delimited {
      Objects.requireNonNull(delimiter, "delimiter");
    }
  }

  record Named(String name) implements ValueAggregation {
    public Named {
      Objects.requireNonNull(name, "name");
    }
  }

  static ValueAggregation firstNonNull() {
    return new FirstNonNull();
  }

  static ValueAggregation exactlyOne() {
    return new ExactlyOne();
  }

  static ValueAggregation pipeDelimitedDistinct() {
    return new Delimited("|", true);
  }

  static ValueAggregation named(String name) {
    return new Named(name);
  }
}
