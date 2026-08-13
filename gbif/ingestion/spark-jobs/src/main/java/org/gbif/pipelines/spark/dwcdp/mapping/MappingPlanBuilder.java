package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Fluent composition API for Event- or Occurrence-core plans. */
public final class MappingPlanBuilder {
  private final String name;
  private final CoreType coreType;
  private final String coreSourceResource;
  private final List<TargetFieldMapping> coreFields = new ArrayList<>();
  private final Map<String, List<ExtensionFragment>> extensions = new LinkedHashMap<>();

  private MappingPlanBuilder(String name, CoreType coreType, String coreSourceResource) {
    this.name = name;
    this.coreType = coreType;
    this.coreSourceResource = coreSourceResource;
  }

  public static MappingPlanBuilder mappingPlan(
      String name, CoreType coreType, String coreSourceResource) {
    return new MappingPlanBuilder(name, coreType, coreSourceResource);
  }

  public MappingPlanBuilder coreField(TargetFieldMapping field) {
    coreFields.add(field);
    return this;
  }

  public ExtensionBuilder extension(String rowType) {
    return new ExtensionBuilder(this, rowType);
  }

  public MappingPlan build() {
    List<ExtensionMapping> builtExtensions =
        extensions.entrySet().stream()
            .map(e -> new ExtensionMapping(e.getKey(), e.getValue()))
            .toList();
    return new MappingPlan(name, coreType, coreSourceResource, coreFields, builtExtensions);
  }

  public static final class ExtensionBuilder {
    private final MappingPlanBuilder parent;
    private final String rowType;

    private ExtensionBuilder(MappingPlanBuilder parent, String rowType) {
      this.parent = parent;
      this.rowType = rowType;
    }

    /** Imports a reusable fragment into this extension. */
    public ExtensionBuilder importFragment(ExtensionFragment fragment) {
      if (!rowType.equals(fragment.rowType())) {
        throw new IllegalArgumentException(
            "Fragment " + fragment.name() + " targets " + fragment.rowType() + ", not " + rowType);
      }
      parent.extensions.computeIfAbsent(rowType, ignored -> new ArrayList<>()).add(fragment);
      return this;
    }

    public MappingPlanBuilder endExtension() {
      return parent;
    }

    public ExtensionBuilder extension(String nextRowType) {
      return parent.extension(nextRowType);
    }

    public MappingPlan build() {
      return parent.build();
    }
  }
}
