package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Materialized extension rows before they are attached to an Event/Occurrence core.
 *
 * <p>Each row has an internal parent/scope key, its logical source {@link FieldRef}, an extension
 * row key, and one
 * physical Spark column per mapped DwC-A term. Consumers use {@link #column(String)} rather than
 * depending on those physical aliases.
 */
public record ExtensionMaterializationResult(
    Dataset<Row> dataset,
    String parentKeyColumn,
    FieldRef parentKeySource,
    String rowKeyColumn,
    Map<String, String> targetColumns) {

  public ExtensionMaterializationResult {
    Objects.requireNonNull(dataset, "dataset");
    Objects.requireNonNull(parentKeyColumn, "parentKeyColumn");
    Objects.requireNonNull(parentKeySource, "parentKeySource");
    Objects.requireNonNull(rowKeyColumn, "rowKeyColumn");
    targetColumns = Map.copyOf(targetColumns);
  }

  public String columnName(String targetTerm) {
    String name = targetColumns.get(targetTerm);
    if (name == null) {
      throw new IllegalArgumentException("Target term is not materialized: " + targetTerm);
    }
    return name;
  }

  public Column column(String targetTerm) {
    return dataset.col(columnName(targetTerm));
  }
}
