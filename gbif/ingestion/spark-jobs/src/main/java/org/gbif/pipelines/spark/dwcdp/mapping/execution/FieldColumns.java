package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Column resolver exposed to mapping filter expressions.
 *
 * <p>This is deliberately not a row abstraction. Calls return ordinary Spark {@link Column}
 * expressions, which become part of Spark's logical/Catalyst plan and are evaluated distributed on
 * executors.
 */
public final class FieldColumns implements Serializable {
  private final Dataset<Row> dataset;

  private FieldColumns(Dataset<Row> dataset) {
    this.dataset = Objects.requireNonNull(dataset, "dataset");
  }

  public static FieldColumns of(Dataset<Row> dataset) {
    return new FieldColumns(dataset);
  }

  /** Resolves a column on the current relation resource. */
  public Column col(String fieldName) {
    Objects.requireNonNull(fieldName, "fieldName");
    if (fieldName.isBlank()) {
      throw new IllegalArgumentException("fieldName must not be blank");
    }
    return dataset.col(fieldName);
  }

  /**
   * Resolves an optional physical column, returning a null literal when the current package omitted
   * it. Mapping definitions still refer to fields from the official schema, but optional schema
   * fields are not guaranteed to be materialized in every Parquet table.
   */
  public Column colOrNull(String fieldName) {
    Objects.requireNonNull(fieldName, "fieldName");
    if (fieldName.isBlank()) {
      throw new IllegalArgumentException("fieldName must not be blank");
    }
    for (String column : dataset.columns()) {
      if (column.equals(fieldName)) {
        return dataset.col(fieldName);
      }
    }
    return org.apache.spark.sql.functions.lit(null);
  }
}
