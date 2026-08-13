package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Objects;
import java.util.Optional;

/** A concrete FK relation discovered from the DwC-DP schema. */
public record SchemaRelation(
    String sourceResource,
    String sourceColumn,
    String targetResource,
    String targetColumn,
    Optional<String> predicate,
    RelationCardinality cardinality) {

  public SchemaRelation {
    Objects.requireNonNull(sourceResource, "sourceResource");
    Objects.requireNonNull(sourceColumn, "sourceColumn");
    Objects.requireNonNull(targetResource, "targetResource");
    Objects.requireNonNull(targetColumn, "targetColumn");
    predicate = predicate == null ? Optional.empty() : predicate;
    Objects.requireNonNull(cardinality, "cardinality");
  }

  public static SchemaRelation relation(
      String sourceResource,
      String sourceColumn,
      String targetResource,
      String targetColumn,
      String predicate,
      RelationCardinality cardinality) {
    return new SchemaRelation(
        sourceResource,
        sourceColumn,
        targetResource,
        targetColumn,
        Optional.ofNullable(predicate),
        cardinality);
  }

  public boolean usesColumn(String column) {
    return sourceColumn.equals(column) || targetColumn.equals(column);
  }
}
