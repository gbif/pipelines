package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Read-only graph of navigable relationships derived from the official DwC-DP schemas. */
public interface SchemaGraph {

  boolean hasResource(String resource);

  boolean hasColumn(String resource, String column);

  default Optional<SchemaResource> resource(String resource) {
    return Optional.empty();
  }

  /** Relations are returned in traversal direction, including reverse traversal of declared FKs. */
  List<SchemaRelation> relations(String sourceResource, String targetResource);

  /** All outgoing relations in traversal direction from a resource. */
  default List<SchemaRelation> relationsFrom(String sourceResource) {
    return List.of();
  }

  /**
   * Bounded breadth-first diagnostic search. These are hints only; callers must never use them to
   * infer or select a mapping relation.
   */
  default List<List<SchemaRelation>> nearbyPaths(
      String sourceResource, String targetResource, int maxDepth, int maxResults) {
    if (maxDepth < 1 || maxResults < 1) {
      return List.of();
    }
    record Candidate(String resource, List<SchemaRelation> path, Set<String> visited) {}
    ArrayDeque<Candidate> queue = new ArrayDeque<>();
    queue.add(new Candidate(sourceResource, List.of(), Set.of(sourceResource)));
    List<List<SchemaRelation>> results = new ArrayList<>();
    while (!queue.isEmpty() && results.size() < maxResults) {
      Candidate candidate = queue.removeFirst();
      if (candidate.path().size() >= maxDepth) {
        continue;
      }
      for (SchemaRelation relation : relationsFrom(candidate.resource())) {
        if (candidate.visited().contains(relation.targetResource())) {
          continue;
        }
        List<SchemaRelation> nextPath = new ArrayList<>(candidate.path());
        nextPath.add(relation);
        if (relation.targetResource().equals(targetResource)) {
          results.add(List.copyOf(nextPath));
          if (results.size() >= maxResults) {
            break;
          }
        }
        Set<String> visited = new HashSet<>(candidate.visited());
        visited.add(relation.targetResource());
        queue.addLast(new Candidate(relation.targetResource(), List.copyOf(nextPath), Set.copyOf(visited)));
      }
    }
    return List.copyOf(results);
  }

  default SchemaRelation resolve(
      String sourceResource, String targetResource, String viaColumn, String predicate) {
    List<SchemaRelation> candidates = relations(sourceResource, targetResource);
    if (viaColumn != null) {
      List<SchemaRelation> sourceMatches =
          candidates.stream().filter(r -> r.sourceColumn().equals(viaColumn)).toList();
      candidates =
          sourceMatches.isEmpty()
              ? candidates.stream().filter(r -> r.usesColumn(viaColumn)).toList()
              : sourceMatches;
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
      List<SchemaRelation> strongCandidates = candidates.stream().filter(r -> !r.weak()).toList();
      if (strongCandidates.size() == 1) {
        return strongCandidates.get(0);
      }
      if (!strongCandidates.isEmpty()) {
        candidates = strongCandidates;
      }
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
