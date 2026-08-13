package org.gbif.pipelines.spark.dwcdp.mapping;

import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;
import org.apache.spark.sql.Column;

/**
 * Builds a native Spark SQL {@link Column} predicate for a mapping relation.
 *
 * <p>The Java lambda is invoked while constructing the logical plan; the returned Spark expression
 * is evaluated distributed by Spark. This is intentionally not a {@code FilterFunction<Row>} and
 * therefore remains visible to Catalyst for normal query optimization.
 *
 * <p>Example: {@code .filter(cols -> cols.col("agentRole").equalTo("collector"))}.
 */
@FunctionalInterface
public interface FilterExpression extends Serializable {

  Column build(FieldColumns columns);

  default boolean isPresent() {
    return true;
  }

  static FilterExpression none() {
    return new FilterExpression() {
      @Override
      public Column build(FieldColumns columns) {
        return lit(true);
      }

      @Override
      public boolean isPresent() {
        return false;
      }
    };
  }
}
