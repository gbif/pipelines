package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        relation = graph.resolve(
            currentResource,
            step.targetResource(),
            step.viaColumn().orElse(null),
            step.schemaPredicate().orElse(null));
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

  private static void error(List<ValidationIssue> issues, String message) {
    issues.add(new ValidationIssue(ValidationIssue.Severity.ERROR, message));
  }
}
