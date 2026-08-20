package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;

/**
 * Immutable mapping traversal that keeps executable relation steps and source-field lineage
 * aligned.
 */
public final class MappingPath {
  private final SchemaGraph graph;
  private final SchemaPath schemaPath;
  private final List<RelationStep> relations;

  private MappingPath(SchemaGraph graph, SchemaPath schemaPath, List<RelationStep> relations) {
    this.graph = Objects.requireNonNull(graph, "graph");
    this.schemaPath = Objects.requireNonNull(schemaPath, "schemaPath");
    this.relations = List.copyOf(relations);
  }

  public static MappingPath root(SchemaGraph graph, String resource) {
    return new MappingPath(graph, SchemaPath.root(resource), List.of());
  }

  public String rootResource() {
    return schemaPath.rootResource();
  }

  public String currentResource() {
    return schemaPath.currentResource();
  }

  public SchemaPath schemaPath() {
    return schemaPath;
  }

  public FieldRef field(String column) {
    return schemaPath.field(column);
  }

  public List<RelationStep> relations() {
    return relations;
  }

  public JoinBuilder join(String targetResource) {
    return new JoinBuilder(this, RelationStep.inferred(targetResource));
  }

  private MappingPath append(RelationStep step) {
    SchemaRelation relation = resolve(step);
    List<RelationStep> nextRelations = new ArrayList<>(relations);
    nextRelations.add(step);
    return new MappingPath(graph, schemaPath.append(relation), nextRelations);
  }

  private SchemaRelation resolve(RelationStep step) {
    if (step.explicitColumns()) {
      String sourceColumn = step.sourceColumn().orElseThrow();
      String targetColumn = step.targetColumn().orElseThrow();
      if (!graph.hasResource(step.targetResource())) {
        throw new IllegalArgumentException(
            "Unknown relation target resource: " + step.targetResource());
      }
      if (!graph.hasColumn(currentResource(), sourceColumn)) {
        throw new IllegalArgumentException(
            "Explicit relation references unknown source field: "
                + currentResource()
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
          currentResource(),
          sourceColumn,
          step.targetResource(),
          targetColumn,
          step.schemaPredicate().orElse(null),
          RelationCardinality.UNKNOWN);
    }
    return graph.resolve(
        currentResource(),
        step.targetResource(),
        step.viaColumn().orElse(null),
        step.schemaPredicate().orElse(null));
  }

  /** Builds one relation step and returns a new immutable path when the step is completed. */
  public static final class JoinBuilder {
    private final MappingPath parent;
    private RelationStep step;

    private JoinBuilder(MappingPath parent, RelationStep step) {
      this.parent = parent;
      this.step = step;
    }

    public JoinBuilder via(String column) {
      step = step.via(column);
      return this;
    }

    public JoinBuilder on(String sourceColumn, String targetColumn) {
      step = step.on(sourceColumn, targetColumn);
      return this;
    }

    public JoinBuilder predicate(String predicate) {
      step = step.predicate(predicate);
      return this;
    }

    public JoinBuilder filter(FilterExpression filter) {
      step = step.filter(filter);
      return this;
    }

    public JoinBuilder optional() {
      step = step.requirement(RelationRequirement.OPTIONAL);
      return this;
    }

    public JoinBuilder required() {
      step = step.requirement(RelationRequirement.REQUIRED);
      return this;
    }

    public MappingPath fanOut() {
      return parent.append(step.with(CardinalityStrategy.fanOut()));
    }

    public MappingPath exactlyOne() {
      return parent.append(step.with(CardinalityStrategy.exactlyOne()));
    }

    public MappingPath select(String selector) {
      return parent.append(step.with(CardinalityStrategy.select(selector)));
    }

    public MappingPath combine(ValueAggregation aggregation) {
      return parent.append(step.with(CardinalityStrategy.combine(aggregation)));
    }

    /** Completes a relation without declaring a mapping-level cardinality strategy. */
    public MappingPath endJoin() {
      return parent.append(step);
    }
  }
}
