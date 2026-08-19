package org.gbif.pipelines.spark.dwcdp.mapping.schema;

import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationStep;

/** Resolves one declarative relation step against the schema graph. */
public final class SchemaRelationResolver {
  private SchemaRelationResolver() {}

  public static SchemaRelation resolve(
      SchemaGraph graph, String sourceResource, RelationStep step) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(sourceResource, "sourceResource");
    Objects.requireNonNull(step, "step");

    if (step.explicitColumns()) {
      String sourceColumn = step.sourceColumn().orElseThrow();
      String targetColumn = step.targetColumn().orElseThrow();
      if (!graph.hasResource(step.targetResource())) {
        throw new IllegalArgumentException(
            "Unknown relation target resource: " + step.targetResource());
      }
      if (!graph.hasColumn(sourceResource, sourceColumn)) {
        throw new IllegalArgumentException(
            "Explicit relation references unknown source field: "
                + sourceResource
                + "."
                + sourceColumn);
      }
      if (!graph.hasColumn(step.targetResource(), targetColumn)) {
        throw new IllegalArgumentException(
            "Explicit relation references unknown target field: "
                + step.targetResource()
                + "."
                + targetColumn);
      }
      return SchemaRelation.relation(
          sourceResource,
          sourceColumn,
          step.targetResource(),
          targetColumn,
          step.schemaPredicate().orElse(null),
          RelationCardinality.UNKNOWN);
    }

    return graph.resolve(
        sourceResource,
        step.targetResource(),
        step.viaColumn().orElse(null),
        step.schemaPredicate().orElse(null));
  }
}
