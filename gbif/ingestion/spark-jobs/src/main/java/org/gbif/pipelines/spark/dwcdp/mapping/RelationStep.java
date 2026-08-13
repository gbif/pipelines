package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Objects;
import java.util.Optional;

/** Requested navigation from the current resource to another resource. */
public record RelationStep(
    String targetResource,
    Optional<String> viaColumn,
    Optional<String> schemaPredicate,
    FilterExpression filter,
    Optional<CardinalityStrategy> cardinalityStrategy,
    RelationRequirement requirement) {

  public RelationStep {
    Objects.requireNonNull(targetResource, "targetResource");
    viaColumn = viaColumn == null ? Optional.empty() : viaColumn;
    schemaPredicate = schemaPredicate == null ? Optional.empty() : schemaPredicate;
    filter = filter == null ? FilterExpression.none() : filter;
    cardinalityStrategy = cardinalityStrategy == null ? Optional.empty() : cardinalityStrategy;
    requirement = requirement == null ? RelationRequirement.OPTIONAL : requirement;
  }

  public static RelationStep inferred(String targetResource) {
    return new RelationStep(
        targetResource,
        Optional.empty(),
        Optional.empty(),
        FilterExpression.none(),
        Optional.empty(),
        RelationRequirement.OPTIONAL);
  }

  public RelationStep via(String column) {
    return new RelationStep(
        targetResource, Optional.of(column), schemaPredicate, filter, cardinalityStrategy, requirement);
  }

  public RelationStep predicate(String predicate) {
    return new RelationStep(
        targetResource, viaColumn, Optional.of(predicate), filter, cardinalityStrategy, requirement);
  }

  public RelationStep filter(FilterExpression newFilter) {
    return new RelationStep(
        targetResource, viaColumn, schemaPredicate, newFilter, cardinalityStrategy, requirement);
  }

  public RelationStep with(CardinalityStrategy strategy) {
    return new RelationStep(
        targetResource, viaColumn, schemaPredicate, filter, Optional.of(strategy), requirement);
  }

  public RelationStep requirement(RelationRequirement newRequirement) {
    return new RelationStep(
        targetResource, viaColumn, schemaPredicate, filter, cardinalityStrategy, newRequirement);
  }
}
