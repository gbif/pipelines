package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;

/** Schema-resolved extension fragment with stable source lineage. */
public record CompiledFragment(
    String name,
    String rowType,
    String sourceResource,
    SchemaPath path,
    List<CompiledRelationStep> relations,
    FieldRef scopeKey,
    Optional<FieldRef> rowIdentity,
    List<CompiledTargetProducer> targets) {

  public CompiledFragment {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(rowType, "rowType");
    Objects.requireNonNull(sourceResource, "sourceResource");
    Objects.requireNonNull(path, "path");
    relations = List.copyOf(relations);
    Objects.requireNonNull(scopeKey, "scopeKey");
    rowIdentity = rowIdentity == null ? Optional.empty() : rowIdentity;
    targets = List.copyOf(targets);
  }
}
