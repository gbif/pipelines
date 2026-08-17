package org.gbif.pipelines.spark.dwcdp.mapping.definition;

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
  private final List<CoreFragment> coreFragments = new ArrayList<>();
  private final Map<String, TargetMerge> coreTargetMerges = new LinkedHashMap<>();
  private final Map<String, List<ExtensionFragment>> extensions = new LinkedHashMap<>();
  private final Map<String, ExtensionRowComposition> extensionCompositions = new LinkedHashMap<>();
  private final Map<String, Integer> extensionRowLimits = new LinkedHashMap<>();
  private final Map<String, Map<String, TargetMerge>> extensionTargetMerges = new LinkedHashMap<>();

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

  public MappingPlanBuilder importCoreFragment(CoreFragment fragment) {
    coreFragments.add(fragment);
    return this;
  }

  /** Explicitly allows independent producers to contribute additively to one core target. */
  public MappingPlanBuilder mergeCoreTarget(String targetTerm, ValueAggregation aggregation) {
    TargetMerge previous = coreTargetMerges.put(targetTerm, new TargetMerge(targetTerm, aggregation));
    if (previous != null && !previous.aggregation().equals(aggregation)) {
      throw new IllegalArgumentException("Conflicting merge semantics for core target: " + targetTerm);
    }
    return this;
  }

  public ExtensionBuilder extension(String rowType) {
    return new ExtensionBuilder(this, rowType);
  }

  public MappingPlan build() {
    List<ExtensionMapping> builtExtensions =
        extensions.entrySet().stream()
            .map(
                e ->
                    new ExtensionMapping(
                        e.getKey(),
                        extensionCompositions.getOrDefault(
                            e.getKey(), ExtensionRowComposition.ENRICH),
                        java.util.Optional.ofNullable(extensionRowLimits.get(e.getKey())),
                        new ArrayList<>(extensionTargetMerges.getOrDefault(e.getKey(), Map.of()).values()),
                        e.getValue()))
            .toList();
    return new MappingPlan(
        name, coreType, coreSourceResource, coreFields, coreFragments,
        new ArrayList<>(coreTargetMerges.values()), builtExtensions);
  }

  public static final class ExtensionBuilder {
    private final MappingPlanBuilder parent;
    private final String rowType;

    private ExtensionBuilder(MappingPlanBuilder parent, String rowType) {
      this.parent = parent;
      this.rowType = rowType;
    }

    /** Makes every imported fragment an independent row producer whose rows are unioned. */
    public ExtensionBuilder unionRows() {
      parent.extensionCompositions.put(rowType, ExtensionRowComposition.UNION);
      return this;
    }

    /** Limits the number of serialized extension rows retained for each parent record. */
    public ExtensionBuilder limitRowsPerParent(int maxRows) {
      if (maxRows <= 0) {
        throw new IllegalArgumentException("maxRows must be > 0");
      }
      parent.extensionRowLimits.put(rowType, maxRows);
      return this;
    }

    /** Explicitly allows independent producers to contribute additively to one extension target. */
    public ExtensionBuilder mergeTarget(String targetTerm, ValueAggregation aggregation) {
      Map<String, TargetMerge> merges =
          parent.extensionTargetMerges.computeIfAbsent(rowType, ignored -> new LinkedHashMap<>());
      TargetMerge previous = merges.put(targetTerm, new TargetMerge(targetTerm, aggregation));
      if (previous != null && !previous.aggregation().equals(aggregation)) {
        throw new IllegalArgumentException(
            "Conflicting merge semantics for extension target " + rowType + ": " + targetTerm);
      }
      return this;
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
