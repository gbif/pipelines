package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.Row;

/** Executed schema path plus the physical aliases for its logical path-qualified fields. */
public record SparkPathResult(Dataset<Row> dataset, Map<FieldRef, String> aliases) {
  public SparkPathResult {
    aliases = Map.copyOf(aliases);
  }

  public String columnName(FieldRef field) {
    String name = aliases.get(field);
    if (name == null) {
      throw new IllegalArgumentException("Field is not materialized by this path: " + field.qualifiedName());
    }
    return name;
  }

  public Column column(FieldRef field) {
    return dataset.col(columnName(field));
  }

  /**
   * Returns the materialized Spark column, or a null literal when the official schema declares the
   * field but this particular package omitted the optional physical column.
   */
  public Column columnOrNull(FieldRef field) {
    String name = aliases.get(field);
    return name == null ? lit(null) : dataset.col(name);
  }
}
