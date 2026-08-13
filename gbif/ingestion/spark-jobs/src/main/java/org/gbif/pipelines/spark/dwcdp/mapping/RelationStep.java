package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Objects;
import java.util.Optional;

/** Requested navigation from the current resource to another resource. */
public record RelationStep(
    String targetResource,
    Optional<String> viaColumn,
    Optional<String> schemaPredicate,
    RowFilter filter,
    Optional<CardinalityStrategy> cardinalityStrategy) {

  public RelationStep {
    Objects.requireNonNull(targetResource, "targetResource");
    viaColumn = viaColumn == null ? Optional.empty() : viaColumn;
    schemaPredicate = schemaPredicate == null ? Optional.empty() : schemaPredicate;
    filter = filter == null ? RowFilter.none() : filter;
    cardinalityStrategy = cardinalityStrategy == null ? Optional.empty() : cardinalityStrategy;
  }

  public static RelationStep inferred(String targetResource) {
    return new RelationStep(
        targetResource, Optional.empty(), Optional.empty(), RowFilter.none(), Optional.empty());
  }

  public RelationStep via(String column) {
    return new RelationStep(
        targetResource, Optional.of(column), schemaPredicate, filter, cardinalityStrategy);
  }

  public RelationStep predicate(String predicate) {
    return new RelationStep(
        targetResource, viaColumn, Optional.of(predicate), filter, cardinalityStrategy);
  }

  public RelationStep filter(RowFilter newFilter) {
    return new RelationStep(
        targetResource, viaColumn, schemaPredicate, newFilter, cardinalityStrategy);
  }

  public RelationStep with(CardinalityStrategy strategy) {
    return new RelationStep(targetResource, viaColumn, schemaPredicate, filter, Optional.of(strategy));
  }
}
