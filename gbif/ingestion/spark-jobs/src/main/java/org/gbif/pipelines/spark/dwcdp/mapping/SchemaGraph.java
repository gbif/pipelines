package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.List;
import java.util.Optional;

/** Read-only graph of navigable relationships derived from the official DwC-DP schemas. */
public interface SchemaGraph {

  boolean hasResource(String resource);

  boolean hasColumn(String resource, String column);

  default Optional<SchemaResource> resource(String resource) {
    return Optional.empty();
  }

  /** Relations are returned in traversal direction, including reverse traversal of declared FKs. */
  List<SchemaRelation> relations(String sourceResource, String targetResource);

  default SchemaRelation resolve(
      String sourceResource, String targetResource, String viaColumn, String predicate) {
    List<SchemaRelation> candidates = relations(sourceResource, targetResource);
    if (viaColumn != null) {
      candidates = candidates.stream().filter(r -> r.usesColumn(viaColumn)).toList();
    }
    if (predicate != null) {
      candidates = candidates.stream()
          .filter(r -> r.predicate().map(predicate::equals).orElse(false))
          .toList();
    }
    if (candidates.isEmpty()) {
      throw new IllegalArgumentException(
          "No schema relation from " + sourceResource + " to " + targetResource
              + (viaColumn == null ? "" : " via " + viaColumn)
              + (predicate == null ? "" : " predicate " + predicate));
    }
    if (candidates.size() > 1) {
      throw new IllegalArgumentException(
          "Ambiguous schema relation from " + sourceResource + " to " + targetResource
              + ": " + candidates);
    }
    return candidates.get(0);
  }

  default SchemaRelation resolve(String sourceResource, String targetResource) {
    return resolve(sourceResource, targetResource, null, null);
  }

  default SchemaRelation resolve(String sourceResource, String targetResource, String viaColumn) {
    return resolve(sourceResource, targetResource, viaColumn, null);
  }
}
