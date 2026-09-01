package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Complete mapping output shape for one DwC-A core choice. */
public record MappingPlan(
    String name,
    CoreType coreType,
    String coreSourceResource,
    Optional<TargetFieldMapping> coreIdentity,
    List<TargetFieldMapping> coreFields,
    List<CoreFragment> coreFragments,
    List<TargetMerge> coreTargetMerges,
    List<ExtensionMapping> extensions,
    List<NestedExtensionContext> nestedExtensionContexts) {

  public MappingPlan(
      String name,
      CoreType coreType,
      String coreSourceResource,
      Optional<TargetFieldMapping> coreIdentity,
      List<TargetFieldMapping> coreFields,
      List<CoreFragment> coreFragments,
      List<TargetMerge> coreTargetMerges,
      List<ExtensionMapping> extensions) {
    this(
        name,
        coreType,
        coreSourceResource,
        coreIdentity,
        coreFields,
        coreFragments,
        coreTargetMerges,
        extensions,
        List.of());
  }

  public MappingPlan {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(coreType, "coreType");
    Objects.requireNonNull(coreSourceResource, "coreSourceResource");
    coreIdentity = coreIdentity == null ? Optional.empty() : coreIdentity;
    coreFields = List.copyOf(coreFields);
    coreFragments = List.copyOf(coreFragments);
    coreTargetMerges = List.copyOf(coreTargetMerges);
    extensions = List.copyOf(extensions);
    nestedExtensionContexts = List.copyOf(nestedExtensionContexts);
  }
}
