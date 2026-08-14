package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.ArrayList;
import java.util.List;

/** Fluent builder for a reusable core enrichment fragment. */
public final class CoreFragmentBuilder {
  private final String name;
  private final String sourceResource;
  private final List<RelationStep> relations = new ArrayList<>();
  private final List<TargetFieldMapping> fields = new ArrayList<>();

  private CoreFragmentBuilder(String name, String sourceResource) {
    this.name = name;
    this.sourceResource = sourceResource;
  }

  public static CoreFragmentBuilder coreFragment(String name, String sourceResource) {
    return new CoreFragmentBuilder(name, sourceResource);
  }

  public RelationBuilder join(String targetResource) {
    return new RelationBuilder(this, RelationStep.inferred(targetResource));
  }

  public CoreFragmentBuilder field(TargetFieldMapping field) {
    fields.add(field);
    return this;
  }

  public CoreFragment build() {
    return new CoreFragment(name, sourceResource, relations, fields);
  }

  public static final class RelationBuilder {
    private final CoreFragmentBuilder parent;
    private RelationStep step;
    private boolean committed;

    private RelationBuilder(CoreFragmentBuilder parent, RelationStep step) {
      this.parent = parent;
      this.step = step;
    }

    public RelationBuilder via(String column) {
      step = step.via(column);
      return this;
    }

    public RelationBuilder on(String sourceColumn, String targetColumn) {
      step = step.on(sourceColumn, targetColumn);
      return this;
    }

    public RelationBuilder predicate(String predicate) {
      step = step.predicate(predicate);
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

    public CoreFragmentBuilder endJoin() {
      commit();
      return parent;
    }

    public RelationBuilder join(String targetResource) {
      commit();
      return parent.join(targetResource);
    }

    public CoreFragmentBuilder field(TargetFieldMapping field) {
      commit();
      return parent.field(field);
    }

    public CoreFragment build() {
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
