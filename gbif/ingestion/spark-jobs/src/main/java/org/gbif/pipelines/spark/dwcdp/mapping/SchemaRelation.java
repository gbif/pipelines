package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Objects;
import java.util.Optional;

/** A concrete relation discovered from the DwC-DP schema. */
public record SchemaRelation(
    String sourceResource,
    String sourceColumn,
    String targetResource,
    String targetColumn,
    Optional<String> predicate,
    RelationCardinality cardinality,
    boolean weak) {

  public SchemaRelation {
    Objects.requireNonNull(sourceResource, "sourceResource");
    Objects.requireNonNull(sourceColumn, "sourceColumn");
    Objects.requireNonNull(targetResource, "targetResource");
    Objects.requireNonNull(targetColumn, "targetColumn");
    predicate = predicate == null ? Optional.empty() : predicate;
    Objects.requireNonNull(cardinality, "cardinality");
  }

  /** Compatibility constructor for ordinary/strong or mapping-defined relations. */
  public SchemaRelation(
      String sourceResource,
      String sourceColumn,
      String targetResource,
      String targetColumn,
      Optional<String> predicate,
      RelationCardinality cardinality) {
    this(sourceResource, sourceColumn, targetResource, targetColumn, predicate, cardinality, false);
  }

  public static SchemaRelation relation(
      String sourceResource,
      String sourceColumn,
      String targetResource,
      String targetColumn,
      String predicate,
      RelationCardinality cardinality) {
    return relation(
        sourceResource, sourceColumn, targetResource, targetColumn, predicate, cardinality, false);
  }

  public static SchemaRelation relation(
      String sourceResource,
      String sourceColumn,
      String targetResource,
      String targetColumn,
      String predicate,
      RelationCardinality cardinality,
      boolean weak) {
    return new SchemaRelation(
        sourceResource,
        sourceColumn,
        targetResource,
        targetColumn,
        Optional.ofNullable(predicate),
        cardinality,
        weak);
  }

  public boolean usesColumn(String column) {
    return sourceColumn.equals(column) || targetColumn.equals(column);
  }
}
