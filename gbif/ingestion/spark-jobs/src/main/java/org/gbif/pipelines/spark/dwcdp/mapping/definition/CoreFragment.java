package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.List;
import java.util.Objects;

/** Reusable enrichment path contributing fields to a DwC-A core record. */
public record CoreFragment(
    String name,
    String sourceResource,
    List<RelationStep> relations,
    List<TargetFieldMapping> fields) {

  public CoreFragment {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(sourceResource, "sourceResource");
    relations = List.copyOf(relations);
    fields = List.copyOf(fields);
  }
}
