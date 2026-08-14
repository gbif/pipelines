package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.CardinalityStrategy;
import org.gbif.pipelines.spark.dwcdp.mapping.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationRequirement;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationStep;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaRelation;

/** Schema-resolved relation plus the execution semantics declared by the mapping. */
public record CompiledRelationStep(
    SchemaRelation relation,
    boolean explicitColumns,
    RelationRequirement requirement,
    Optional<CardinalityStrategy> cardinalityStrategy,
    FilterExpression filter) {

  public CompiledRelationStep {
    Objects.requireNonNull(relation, "relation");
    Objects.requireNonNull(requirement, "requirement");
    cardinalityStrategy = cardinalityStrategy == null ? Optional.empty() : cardinalityStrategy;
    filter = filter == null ? FilterExpression.none() : filter;
  }

  public RelationStep toRelationStep() {
    if (explicitColumns) {
      return new RelationStep(
          relation.targetResource(),
          Optional.empty(),
          relation.predicate(),
          Optional.of(relation.sourceColumn()),
          Optional.of(relation.targetColumn()),
          filter,
          cardinalityStrategy,
          requirement);
    }

    // Schema-declared relations must be re-resolved through SchemaGraph during execution.
    // Reconstructing them as explicit column relations loses schema relation metadata such as
    // cardinality, which is part of SchemaPath identity and breaks compiled FieldRef bindings.
    return new RelationStep(
        relation.targetResource(),
        Optional.of(relation.sourceColumn()),
        relation.predicate(),
        Optional.empty(),
        Optional.empty(),
        filter,
        cardinalityStrategy,
        requirement);
  }

  public String describe() {
    StringBuilder out = new StringBuilder();
    out.append(relation.sourceResource())
        .append('.')
        .append(relation.sourceColumn())
        .append(" -> ")
        .append(relation.targetResource())
        .append('.')
        .append(relation.targetColumn());
    relation.predicate().ifPresent(predicate -> out.append(" [predicate=").append(predicate).append(']'));
    out.append(" [").append(requirement).append(']');
    cardinalityStrategy.ifPresent(strategy -> out.append(" [").append(strategy).append(']'));
    if (filter.isPresent()) {
      out.append(" [filter=Spark expression]");
    }
    return out.toString();
  }
}
