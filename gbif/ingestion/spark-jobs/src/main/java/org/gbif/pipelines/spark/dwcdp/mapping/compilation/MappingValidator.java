package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldSource;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.Mapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationStep;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;

/** Static validation of a mapping against the schema graph, with no Spark execution required. */
public final class MappingValidator {
  private MappingValidator() {}

  public static ValidationResult validate(Mapping mapping, SchemaGraph graph) {
    List<ValidationIssue> issues = new ArrayList<>();

    if (!graph.hasResource(mapping.sourceResource())) {
      error(issues, "Unknown source resource: " + mapping.sourceResource());
      return new ValidationResult(issues);
    }

    Set<String> reachable = new HashSet<>();
    reachable.add(mapping.sourceResource());
    String currentResource = mapping.sourceResource();

    for (RelationStep step : mapping.relations()) {
      SchemaRelation relation;
      try {
        relation = resolveRelation(graph, currentResource, step);
      } catch (IllegalArgumentException e) {
        error(issues, e.getMessage());
        currentResource = step.targetResource();
        reachable.add(currentResource);
        continue;
      }

      if (relation.cardinality().canMultiplyRows() && step.cardinalityStrategy().isEmpty()) {
        error(
            issues,
            "Relation "
                + relation.sourceResource()
                + " -> "
                + relation.targetResource()
                + " can multiply rows ("
                + relation.cardinality()
                + ") but has no explicit cardinality strategy");
      }

      currentResource = step.targetResource();
      reachable.add(currentResource);
    }

    for (TargetMapping target : mapping.targets()) {
      for (FieldSource source : target.sources()) {
        if (!reachable.contains(source.resource())) {
          error(
              issues,
              "Target "
                  + target.targetField()
                  + " references resource outside mapping path: "
                  + source.resource());
        } else if (!graph.hasColumn(source.resource(), source.column())) {
          error(
              issues,
              "Target "
                  + target.targetField()
                  + " references unknown field: "
                  + source.resource()
                  + "."
                  + source.column());
        }
      }
    }

    return new ValidationResult(issues);
  }

  private static SchemaRelation resolveRelation(
      SchemaGraph graph, String sourceResource, RelationStep step) {
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

  private static void error(List<ValidationIssue> issues, String message) {
    issues.add(new ValidationIssue(ValidationIssue.Severity.ERROR, message));
  }
}
