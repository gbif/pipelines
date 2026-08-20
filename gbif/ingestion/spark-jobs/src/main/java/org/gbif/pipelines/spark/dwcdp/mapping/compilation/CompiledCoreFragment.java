package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.List;
import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;

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
