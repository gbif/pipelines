package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.List;
import java.util.Objects;

/** Declarative description of a DwC-DP mapping path and its output fields. */
public record Mapping(
    String name,
    String sourceResource,
    List<RelationStep> relations,
    List<TargetMapping> targets,
    Projection projection) {

  public Mapping {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(sourceResource, "sourceResource");
    relations = List.copyOf(relations);
    targets = List.copyOf(targets);
    projection = projection == null ? Projection.none() : projection;
  }
}
