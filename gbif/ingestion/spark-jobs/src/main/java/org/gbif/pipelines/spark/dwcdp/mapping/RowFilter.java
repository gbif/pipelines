package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Objects;

/** Row-level filter attached to a mapping relation. Kept separate from schema relation predicates. */
public record RowFilter(String expression) {
  private static final RowFilter NONE = new RowFilter("");

  public RowFilter {
    Objects.requireNonNull(expression, "expression");
  }

  public static RowFilter none() {
    return NONE;
  }

  public static RowFilter expression(String expression) {
    return new RowFilter(expression);
  }

  public boolean isPresent() {
    return !expression.isBlank();
  }
}
