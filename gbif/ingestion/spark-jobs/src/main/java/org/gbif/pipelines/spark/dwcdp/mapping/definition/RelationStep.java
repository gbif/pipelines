package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.Objects;
import java.util.Optional;

/** Requested navigation from the current resource to another resource. */
public record RelationStep(
    String targetResource,
    Optional<String> viaColumn,
    Optional<String> schemaPredicate,
    Optional<String> sourceColumn,
    Optional<String> targetColumn,
    FilterExpression filter,
    Optional<CardinalityStrategy> cardinalityStrategy,
    RelationRequirement requirement) {

  public RelationStep {
    Objects.requireNonNull(targetResource, "targetResource");
    viaColumn = viaColumn == null ? Optional.empty() : viaColumn;
    schemaPredicate = schemaPredicate == null ? Optional.empty() : schemaPredicate;
    sourceColumn = sourceColumn == null ? Optional.empty() : sourceColumn;
    targetColumn = targetColumn == null ? Optional.empty() : targetColumn;
    filter = filter == null ? FilterExpression.none() : filter;
    cardinalityStrategy = cardinalityStrategy == null ? Optional.empty() : cardinalityStrategy;
    requirement = requirement == null ? RelationRequirement.OPTIONAL : requirement;
    if (sourceColumn.isPresent() != targetColumn.isPresent()) {
      throw new IllegalArgumentException(
          "Explicit relation requires both source and target columns");
    }
  }

  public static RelationStep inferred(String targetResource) {
    return new RelationStep(
        targetResource,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        FilterExpression.none(),
        Optional.empty(),
        RelationRequirement.OPTIONAL);
  }

  public RelationStep via(String column) {
    return new RelationStep(
        targetResource,
        Optional.of(column),
        schemaPredicate,
        sourceColumn,
        targetColumn,
        filter,
        cardinalityStrategy,
        requirement);
  }

  public RelationStep predicate(String predicate) {
    return new RelationStep(
        targetResource,
        viaColumn,
        Optional.of(predicate),
        sourceColumn,
        targetColumn,
        filter,
        cardinalityStrategy,
        requirement);
  }

  /** Declares a mapping-defined relation not present as a schema FK, e.g. a weak/natural-key link. */
  public RelationStep on(String fromColumn, String toColumn) {
    return new RelationStep(
        targetResource,
        viaColumn,
        schemaPredicate,
        Optional.of(fromColumn),
        Optional.of(toColumn),
        filter,
        cardinalityStrategy,
        requirement);
  }

  public boolean explicitColumns() {
    return sourceColumn.isPresent();
  }

  public RelationStep filter(FilterExpression newFilter) {
    return new RelationStep(
        targetResource,
        viaColumn,
        schemaPredicate,
        sourceColumn,
        targetColumn,
        newFilter,
        cardinalityStrategy,
        requirement);
  }

  public RelationStep with(CardinalityStrategy strategy) {
    return new RelationStep(
        targetResource,
        viaColumn,
        schemaPredicate,
        sourceColumn,
        targetColumn,
        filter,
        Optional.of(strategy),
        requirement);
  }

  public RelationStep requirement(RelationRequirement newRequirement) {
    return new RelationStep(
        targetResource,
        viaColumn,
        schemaPredicate,
        sourceColumn,
        targetColumn,
        filter,
        cardinalityStrategy,
        newRequirement);
  }
}
