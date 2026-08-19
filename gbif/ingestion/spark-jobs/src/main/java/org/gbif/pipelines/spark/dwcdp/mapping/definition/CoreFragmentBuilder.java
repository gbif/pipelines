package org.gbif.pipelines.spark.dwcdp.mapping.definition;

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

  /** Creates a fragment using the root and relation steps already captured by {@code path}. */
  public static CoreFragmentBuilder coreFragment(String name, MappingPath path) {
    CoreFragmentBuilder builder = new CoreFragmentBuilder(name, path.rootResource());
    builder.relations.addAll(path.relations());
    return builder;
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

    public CoreFragmentBuilder endJoin() {
      commit();
      return parent;
    }

    private void commit() {
      if (!committed) {
        parent.relations.add(step);
        committed = true;
      }
    }
  }
}
