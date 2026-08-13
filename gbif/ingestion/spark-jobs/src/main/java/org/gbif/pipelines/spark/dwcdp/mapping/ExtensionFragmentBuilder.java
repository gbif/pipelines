package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Fluent builder for a reusable DwC-A extension mapping fragment. */
public final class ExtensionFragmentBuilder {
  private final String name;
  private final String rowType;
  private final String sourceResource;
  private final List<RelationStep> relations = new ArrayList<>();
  private final List<TargetFieldMapping> fields = new ArrayList<>();
  private Optional<String> rowIdentityColumn = Optional.empty();

  private ExtensionFragmentBuilder(String name, String rowType, String sourceResource) {
    this.name = name;
    this.rowType = rowType;
    this.sourceResource = sourceResource;
  }

  public static ExtensionFragmentBuilder extensionFragment(
      String name, String rowType, String sourceResource) {
    return new ExtensionFragmentBuilder(name, rowType, sourceResource);
  }

  public RelationBuilder join(String targetResource) {
    return new RelationBuilder(this, RelationStep.inferred(targetResource));
  }

  public ExtensionFragmentBuilder rowIdentity(String column) {
    this.rowIdentityColumn = Optional.of(column);
    return this;
  }

  public ExtensionFragmentBuilder field(TargetFieldMapping field) {
    fields.add(field);
    return this;
  }

  public ExtensionFragment build() {
    return new ExtensionFragment(name, rowType, sourceResource, relations, rowIdentityColumn, fields);
  }

  public static final class RelationBuilder {
    private final ExtensionFragmentBuilder parent;
    private RelationStep step;
    private boolean committed;

    private RelationBuilder(ExtensionFragmentBuilder parent, RelationStep step) {
      this.parent = parent;
      this.step = step;
    }

    public RelationBuilder via(String column) { step = step.via(column); return this; }
    public RelationBuilder predicate(String predicate) { step = step.predicate(predicate); return this; }
    public RelationBuilder filter(FilterExpression filter) { step = step.filter(filter); return this; }
    public RelationBuilder fanOut() { step = step.with(CardinalityStrategy.fanOut()); return this; }
    public RelationBuilder exactlyOne() { step = step.with(CardinalityStrategy.exactlyOne()); return this; }
    public RelationBuilder select(String selector) { step = step.with(CardinalityStrategy.select(selector)); return this; }
    public RelationBuilder combine(ValueAggregation aggregation) { step = step.with(CardinalityStrategy.combine(aggregation)); return this; }

    public RelationBuilder join(String targetResource) {
      commit();
      return parent.join(targetResource);
    }

    public ExtensionFragmentBuilder rowIdentity(String column) {
      commit();
      return parent.rowIdentity(column);
    }

    public ExtensionFragmentBuilder field(TargetFieldMapping field) {
      commit();
      return parent.field(field);
    }

    public ExtensionFragment build() {
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
