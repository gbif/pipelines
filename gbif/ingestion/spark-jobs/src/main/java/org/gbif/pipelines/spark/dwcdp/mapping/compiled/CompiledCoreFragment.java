package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.List;
import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;

/** Schema-resolved core enrichment fragment with stable source lineage. */
public record CompiledCoreFragment(
    String name,
    String sourceResource,
    SchemaPath path,
    List<CompiledRelationStep> relations,
    List<CompiledTargetProducer> targets) {

  public CompiledCoreFragment {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(sourceResource, "sourceResource");
    Objects.requireNonNull(path, "path");
    relations = List.copyOf(relations);
    targets = List.copyOf(targets);
  }
}
