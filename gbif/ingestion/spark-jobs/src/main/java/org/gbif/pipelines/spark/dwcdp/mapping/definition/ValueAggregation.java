package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.Objects;

/** How several candidate values are reduced when populating one target field. */
public sealed interface ValueAggregation
    permits ValueAggregation.FirstNonNull,
        ValueAggregation.ExactlyOne,
        ValueAggregation.Delimited,
        ValueAggregation.LabeledOrFallback,
        ValueAggregation.PreferredLabeledOrFallback,
        ValueAggregation.Named {

  record FirstNonNull() implements ValueAggregation {}

  record ExactlyOne() implements ValueAggregation {}

  record Delimited(String delimiter, boolean distinct) implements ValueAggregation {
    public Delimited {
      Objects.requireNonNull(delimiter, "delimiter");
    }
  }

  /** Uses label + separator + name when both are present, otherwise the fallback source. */
  record LabeledOrFallback(String separator) implements ValueAggregation {
    public LabeledOrFallback {
      Objects.requireNonNull(separator, "separator");
    }
  }

  /** Preferred source wins; otherwise uses label + separator + name, then fallbacks in order. */
  record PreferredLabeledOrFallback(String separator) implements ValueAggregation {
    public PreferredLabeledOrFallback {
      Objects.requireNonNull(separator, "separator");
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

  static ValueAggregation pipeDelimited() {
    return new Delimited("|", false);
  }

  static ValueAggregation pipeDelimitedDistinct() {
    return new Delimited("|", true);
  }

  static ValueAggregation labeledOrFallback(String separator) {
    return new LabeledOrFallback(separator);
  }

  static ValueAggregation preferredLabeledOrFallback(String separator) {
    return new PreferredLabeledOrFallback(separator);
  }

  static ValueAggregation named(String name) {
    return new Named(name);
  }
}
