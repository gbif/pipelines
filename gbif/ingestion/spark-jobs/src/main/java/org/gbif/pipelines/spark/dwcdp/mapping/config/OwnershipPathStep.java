package org.gbif.pipelines.spark.dwcdp.mapping.config;

import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationRequirement;

/** One reusable ownership traversal step used while composing mapping configuration paths. */
record OwnershipPathStep(
    String resource,
    String viaColumn,
    RelationRequirement requirement,
    Cardinality cardinality,
    Optional<FilterExpression> filter) {

  enum Cardinality {
    FAN_OUT,
    EXACTLY_ONE
  }

  OwnershipPathStep {
    Objects.requireNonNull(resource, "resource");
    Objects.requireNonNull(viaColumn, "viaColumn");
    Objects.requireNonNull(requirement, "requirement");
    Objects.requireNonNull(cardinality, "cardinality");
    Objects.requireNonNull(filter, "filter");
  }

  static OwnershipPathStep fanOut(String resource, String viaColumn) {
    return new OwnershipPathStep(
        resource, viaColumn, RelationRequirement.OPTIONAL, Cardinality.FAN_OUT, Optional.empty());
  }

  static OwnershipPathStep exactlyOne(String resource, String viaColumn) {
    return new OwnershipPathStep(
        resource,
        viaColumn,
        RelationRequirement.OPTIONAL,
        Cardinality.EXACTLY_ONE,
        Optional.empty());
  }

  OwnershipPathStep filter(FilterExpression expression) {
    return new OwnershipPathStep(
        resource,
        viaColumn,
        requirement,
        cardinality,
        Optional.of(Objects.requireNonNull(expression, "expression")));
  }

  MappingPath appendTo(MappingPath current) {
    Objects.requireNonNull(current, "current");
    MappingPath.JoinBuilder relation = current.join(resource).via(viaColumn);
    filter.ifPresent(relation::filter);
    if (requirement == RelationRequirement.OPTIONAL) {
      relation.optional();
    } else {
      relation.required();
    }
    return cardinality == Cardinality.EXACTLY_ONE ? relation.exactlyOne() : relation.fanOut();
  }
}
