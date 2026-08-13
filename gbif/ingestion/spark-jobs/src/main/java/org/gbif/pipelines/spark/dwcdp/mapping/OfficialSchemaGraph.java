package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Immutable graph produced from a DwC-DP schema bundle. */
public final class OfficialSchemaGraph implements SchemaGraph {
  private final Map<String, SchemaResource> resources;
  private final List<SchemaRelation> relations;

  OfficialSchemaGraph(Map<String, SchemaResource> resources, List<SchemaRelation> relations) {
    this.resources = Map.copyOf(resources);
    this.relations = List.copyOf(relations);
  }

  @Override
  public boolean hasResource(String resource) {
    return resources.containsKey(resource);
  }

  @Override
  public boolean hasColumn(String resource, String column) {
    SchemaResource r = resources.get(resource);
    return r != null && r.fields().containsKey(column);
  }

  @Override
  public Optional<SchemaResource> resource(String resource) {
    return Optional.ofNullable(resources.get(resource));
  }

  @Override
  public List<SchemaRelation> relations(String sourceResource, String targetResource) {
    return relations.stream()
        .filter(r -> r.sourceResource().equals(sourceResource)
            && r.targetResource().equals(targetResource))
        .toList();
  }
}
