package org.gbif.pipelines.spark.dwcdp.mapping.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;

/**
 * Logical lineage through DwC-DP resources. Relation identity, not column aliases, defines the
 * path.
 */
public record SchemaPath(String rootResource, List<SchemaRelation> relations) {
  public SchemaPath {
    Objects.requireNonNull(rootResource, "rootResource");
    relations = List.copyOf(relations);
  }

  public static SchemaPath root(String resource) {
    return new SchemaPath(resource, List.of());
  }

  public SchemaPath append(SchemaRelation relation) {
    String current = currentResource();
    if (!relation.sourceResource().equals(current)) {
      throw new IllegalArgumentException(
          "Relation starts at " + relation.sourceResource() + " but path is at " + current);
    }
    List<SchemaRelation> next = new ArrayList<>(relations);
    next.add(relation);
    return new SchemaPath(rootResource, next);
  }

  public String currentResource() {
    return relations.isEmpty()
        ? rootResource
        : relations.get(relations.size() - 1).targetResource();
  }

  public FieldRef field(String column) {
    return new FieldRef(this, column);
  }
}
