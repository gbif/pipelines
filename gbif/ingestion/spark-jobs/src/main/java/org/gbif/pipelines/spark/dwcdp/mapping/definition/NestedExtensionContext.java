package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Declarative parent -> nested-row scope with an optional exactly-one contextual resource.
 *
 * <p>The execution engine treats every resource and column as opaque configuration. Domain meaning
 * belongs to mapping.config.
 */
public record NestedExtensionContext(
    String extensionRowType,
    String parentResource,
    String rowResource,
    String contextResource,
    FieldRef parentIdentity,
    FieldRef rowIdentity,
    FieldRef rowParentKey,
    FieldRef rowContextLink,
    FieldRef contextIdentity,
    FieldRef contextRowLink,
    Optional<String> parentIdentityTargetTerm,
    List<NestedContextDiscoveryFragment> discoveryFragments,
    List<ExtensionFragment> contextualFragments) {

  public NestedExtensionContext {
    Objects.requireNonNull(extensionRowType, "extensionRowType");
    Objects.requireNonNull(parentResource, "parentResource");
    Objects.requireNonNull(rowResource, "rowResource");
    Objects.requireNonNull(contextResource, "contextResource");
    Objects.requireNonNull(parentIdentity, "parentIdentity");
    Objects.requireNonNull(rowIdentity, "rowIdentity");
    Objects.requireNonNull(rowParentKey, "rowParentKey");
    Objects.requireNonNull(rowContextLink, "rowContextLink");
    Objects.requireNonNull(contextIdentity, "contextIdentity");
    Objects.requireNonNull(contextRowLink, "contextRowLink");
    parentIdentityTargetTerm =
        parentIdentityTargetTerm == null ? Optional.empty() : parentIdentityTargetTerm;
    discoveryFragments = List.copyOf(discoveryFragments);
    contextualFragments = List.copyOf(contextualFragments);
    if (discoveryFragments.isEmpty()) {
      throw new IllegalArgumentException("Nested extension context requires discovery fragments");
    }
  }

  public Set<String> contextualFragmentNames() {
    return contextualFragments.stream().map(ExtensionFragment::name).collect(Collectors.toSet());
  }
}
