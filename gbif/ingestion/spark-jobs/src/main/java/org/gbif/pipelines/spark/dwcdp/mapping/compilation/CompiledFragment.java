package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;

/** Schema-resolved extension fragment with stable source lineage. */
public record CompiledFragment(
    String name,
    String rowType,
    String sourceResource,
    SchemaPath path,
    List<CompiledRelationStep> relations,
    FieldRef scopeKey,
    Optional<FieldRef> rowIdentity,
    Optional<FieldRef> rowMatch,
    List<CompiledTargetProducer> targets) {

  public CompiledFragment {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(rowType, "rowType");
    Objects.requireNonNull(sourceResource, "sourceResource");
    Objects.requireNonNull(path, "path");
    relations = List.copyOf(relations);
    Objects.requireNonNull(scopeKey, "scopeKey");
    rowIdentity = rowIdentity == null ? Optional.empty() : rowIdentity;
    rowMatch = rowMatch == null ? Optional.empty() : rowMatch;
    targets = List.copyOf(targets);
  }
}
