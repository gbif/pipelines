package org.gbif.pipelines.spark.dwcdp.mapping.definition;

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
  private Optional<String> scopeKeyColumn = Optional.empty();
  private Optional<FieldRef> rowIdentity = Optional.empty();
  private Optional<FieldRef> rowMatch = Optional.empty();

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

  /**
   * Sets the source-scope key used to attach/materialize this fragment. Defaults to the source
   * resource primary key when one exists. Keyless child/junction resources can point at the FK
   * identifying their parent scope instead.
   */
  public ExtensionFragmentBuilder scopeKey(String column) {
    this.scopeKeyColumn = Optional.of(column);
    return this;
  }

  public ExtensionFragmentBuilder rowIdentity(FieldRef field) {
    this.rowIdentity = Optional.of(field);
    return this;
  }

  /** Declares the existing extension-row identity this enrichment fragment matches. */
  public ExtensionFragmentBuilder rowMatch(FieldRef field) {
    this.rowMatch = Optional.of(field);
    return this;
  }

  public ExtensionFragmentBuilder field(TargetFieldMapping field) {
    fields.add(field);
    return this;
  }

  public ExtensionFragment build() {
    return new ExtensionFragment(
        name, rowType, sourceResource, relations, scopeKeyColumn, rowIdentity, rowMatch, fields);
  }

  public static final class RelationBuilder {
    private final ExtensionFragmentBuilder parent;
    private RelationStep step;
    private boolean committed;

    private RelationBuilder(ExtensionFragmentBuilder parent, RelationStep step) {
      this.parent = parent;
      this.step = step;
    }

    public RelationBuilder via(String column) {
      step = step.via(column);
      return this;
    }

    /**
     * Declares an explicit source-column -> target-column relation (for weak/natural-key links).
     */
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

    public RelationBuilder fanOut() {
      step = step.with(CardinalityStrategy.fanOut());
      return this;
    }

    public RelationBuilder exactlyOne() {
      step = step.with(CardinalityStrategy.exactlyOne());
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

    public RelationBuilder select(String selector) {
      step = step.with(CardinalityStrategy.select(selector));
      return this;
    }

    public RelationBuilder combine(ValueAggregation aggregation) {
      step = step.with(CardinalityStrategy.combine(aggregation));
      return this;
    }

    /** Commits this relation and returns to the fragment builder. */
    public ExtensionFragmentBuilder endJoin() {
      commit();
      return parent;
    }

    public RelationBuilder join(String targetResource) {
      commit();
      return parent.join(targetResource);
    }

    public ExtensionFragmentBuilder scopeKey(String column) {
      commit();
      return parent.scopeKey(column);
    }

    public ExtensionFragmentBuilder rowIdentity(FieldRef field) {
      commit();
      return parent.rowIdentity(field);
    }

    public ExtensionFragmentBuilder rowMatch(FieldRef field) {
      commit();
      return parent.rowMatch(field);
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
