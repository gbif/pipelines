package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.Objects;
import java.util.Optional;

/** One declarative path contributing parent/row/context identities to a nested extension scope. */
public record NestedContextDiscoveryFragment(
    String name,
    MappingPath path,
    FieldRef parentIdentity,
    FieldRef rowIdentity,
    Optional<FieldRef> contextIdentity) {

  public NestedContextDiscoveryFragment {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(parentIdentity, "parentIdentity");
    Objects.requireNonNull(rowIdentity, "rowIdentity");
    contextIdentity = contextIdentity == null ? Optional.empty() : contextIdentity;
  }
}
