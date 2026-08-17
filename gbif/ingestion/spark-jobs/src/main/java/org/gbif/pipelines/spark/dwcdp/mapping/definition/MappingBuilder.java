package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.ArrayList;
import java.util.List;

/** Minimal fluent DSL that constructs a {@link Mapping}; it does not execute Spark operations. */
public final class MappingBuilder {
  private final String name;
  private final String sourceResource;
  private final List<RelationStep> relations = new ArrayList<>();
  private final List<TargetMapping> targets = new ArrayList<>();
  private Projection projection = Projection.none();

  private MappingBuilder(String name, String sourceResource) {
    this.name = name;
    this.sourceResource = sourceResource;
  }

  public static MappingBuilder mapping(String name, String sourceResource) {
    return new MappingBuilder(name, sourceResource);
  }

  public RelationBuilder join(String targetResource) {
    return new RelationBuilder(this, RelationStep.inferred(targetResource));
  }

  public MappingBuilder target(TargetMapping target) {
    targets.add(target);
    return this;
  }

  public MappingBuilder projection(Projection projection) {
    this.projection = projection;
    return this;
  }

  public Mapping build() {
    return new Mapping(name, sourceResource, relations, targets, projection);
  }

  public static final class RelationBuilder {
    private final MappingBuilder parent;
    private RelationStep step;
    private boolean committed;

    private RelationBuilder(MappingBuilder parent, RelationStep step) {
      this.parent = parent;
      this.step = step;
    }

    public RelationBuilder via(String column) {
      step = step.via(column);
      return this;
    }

    /** Declares an explicit source-column -> target-column relation (for weak/natural-key links). */
    public RelationBuilder on(String sourceColumn, String targetColumn) {
      step = step.on(sourceColumn, targetColumn);
      return this;
    }

    public RelationBuilder filter(FilterExpression filter) {
      step = step.filter(filter);
      return this;
    }

    public RelationBuilder optional() {
      step = step.requirement(RelationRequirement.OPTIONAL);
      return this;
    }

    public RelationBuilder required() {
      step = step.requirement(RelationRequirement.REQUIRED);
      return this;
    }

    public RelationBuilder predicate(String predicate) {
      step = step.predicate(predicate);
      return this;
    }

    public RelationBuilder fanOut() {
      step = step.with(CardinalityStrategy.fanOut());
      return this;
    }

    public RelationBuilder exactlyOne() {
      step = step.with(CardinalityStrategy.exactlyOne());
      return this;
    }

    public RelationBuilder select(String selector) {
      step = step.with(CardinalityStrategy.select(selector));
      return this;
    }

    public RelationBuilder combine(ValueAggregation aggregation) {
      step = step.with(CardinalityStrategy.combine(aggregation));
      return this;
    }

    public RelationBuilder join(String targetResource) {
      commit();
      return parent.join(targetResource);
    }

    public MappingBuilder target(TargetMapping target) {
      commit();
      return parent.target(target);
    }

    public Mapping build() {
      commit();
      return parent.build();
    }

    private void commit() {
      if (!committed) {
        parent.relations.add(step);
        committed = true;
      }
    }
  }
}
