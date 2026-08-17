package org.gbif.pipelines.spark.dwcdp.mapping.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;

/**
 * Small graph implementation useful for tests and as the target of a future DataPackage adapter.
 * Relations are registered in traversal direction; add the reverse explicitly when needed.
 */
public final class InMemorySchemaGraph implements SchemaGraph {
  private final Map<String, Set<String>> columns = new HashMap<>();
  private final List<SchemaRelation> relations = new ArrayList<>();

  public InMemorySchemaGraph resource(String name, String... resourceColumns) {
    Set<String> knownColumns = columns.computeIfAbsent(name, ignored -> new HashSet<>());
    knownColumns.addAll(List.of(resourceColumns));
    return this;
  }

  public InMemorySchemaGraph relation(SchemaRelation relation) {
    relations.add(relation);
    resource(relation.sourceResource(), relation.sourceColumn());
    resource(relation.targetResource(), relation.targetColumn());
    return this;
  }

  @Override
  public boolean hasResource(String resource) {
    return columns.containsKey(resource);
  }

  @Override
  public Optional<SchemaResource> resource(String resource) {
    Set<String> names = columns.get(resource);
    if (names == null) return Optional.empty();
    Map<String, SchemaField> fields = new HashMap<>();
    names.forEach(n -> fields.put(n, new SchemaField(n, false, false)));
    return Optional.of(new SchemaResource(resource, fields, Optional.empty(), Optional.empty()));
  }

  @Override
  public boolean hasColumn(String resource, String column) {
    return columns.getOrDefault(resource, Set.of()).contains(column);
  }

  @Override
  public List<SchemaRelation> relationsFrom(String sourceResource) {
    return relations.stream()
        .filter(r -> r.sourceResource().equals(sourceResource))
        .toList();
  }

  @Override
  public List<SchemaRelation> relations(String sourceResource, String targetResource) {
    return relations.stream()
        .filter(
            r ->
                r.sourceResource().equals(sourceResource)
                    && r.targetResource().equals(targetResource))
        .toList();
  }
}
