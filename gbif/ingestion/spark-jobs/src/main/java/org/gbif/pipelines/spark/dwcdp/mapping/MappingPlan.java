package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.List;
import java.util.Objects;

/** Complete mapping output shape for one DwC-A core choice. */
public record MappingPlan(
    String name,
    CoreType coreType,
    String coreSourceResource,
    List<TargetFieldMapping> coreFields,
    List<CoreFragment> coreFragments,
    List<TargetMerge> coreTargetMerges,
    List<ExtensionMapping> extensions) {

  public MappingPlan {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(coreType, "coreType");
    Objects.requireNonNull(coreSourceResource, "coreSourceResource");
    coreFields = List.copyOf(coreFields);
    coreFragments = List.copyOf(coreFragments);
    coreTargetMerges = List.copyOf(coreTargetMerges);
    extensions = List.copyOf(extensions);
  }
}
