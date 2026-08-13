package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Reusable description of extension rows, intentionally independent of Event-vs-Occurrence core.
 * Attachment to a concrete core is supplied by the importing plan/executor.
 */
public record ExtensionFragment(
    String name,
    String rowType,
    String sourceResource,
    List<RelationStep> relations,
    Optional<String> rowIdentityColumn,
    List<TargetFieldMapping> fields) {

  public ExtensionFragment {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(rowType, "rowType");
    Objects.requireNonNull(sourceResource, "sourceResource");
    relations = List.copyOf(relations);
    rowIdentityColumn = rowIdentityColumn == null ? Optional.empty() : rowIdentityColumn;
    fields = List.copyOf(fields);
  }
}
