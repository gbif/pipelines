package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.List;
import java.util.Objects;

/** Complete mapping output shape for one DwC-A core choice. */
public record MappingPlan(
    String name,
    CoreType coreType,
    String coreSourceResource,
    List<TargetFieldMapping> coreFields,
    List<ExtensionMapping> extensions) {

  public MappingPlan {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(coreType, "coreType");
    Objects.requireNonNull(coreSourceResource, "coreSourceResource");
    coreFields = List.copyOf(coreFields);
    extensions = List.copyOf(extensions);
  }
}
