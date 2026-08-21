package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.storage.StorageLevel;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CardinalityStrategy;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationStep;

/**
 * Execution-local cache for deterministic, unfiltered mapping-path prefixes.
 *
 * <p>The first branch only registers the logical prefix. When another branch asks for the same
 * prefix, the shared Dataset is persisted with MEMORY_AND_DISK and reused. This avoids caching
 * one-off paths while allowing Spark to materialize genuinely shared join prefixes once.
 */
public final class SparkPathPrefixCache implements AutoCloseable {
  private final boolean enabled;
  private final Map<PrefixKey, Entry> entries = new LinkedHashMap<>();

  private SparkPathPrefixCache(boolean enabled) {
    this.enabled = enabled;
  }

  public static SparkPathPrefixCache disabled() {
    return new SparkPathPrefixCache(false);
  }

  public static SparkPathPrefixCache enabled() {
    return new SparkPathPrefixCache(true);
  }

  Optional<Hit> longest(String sourceResource, List<RelationStep> relations) {
    if (!enabled) {
      return Optional.empty();
    }
    for (int length = relations.size(); length > 0; length--) {
      List<RelationStep> prefix = relations.subList(0, length);
      if (!cacheable(prefix)) {
        continue;
      }
      Entry entry = entries.get(PrefixKey.of(sourceResource, prefix));
      if (entry != null) {
        if (!entry.persisted) {
          entry.result.dataset().persist(StorageLevel.MEMORY_AND_DISK());
          entry.persisted = true;
        }
        return Optional.of(new Hit(length, entry.result, entry.metrics));
      }
    }
    return Optional.empty();
  }

  void remember(
      String sourceResource,
      List<RelationStep> prefix,
      SparkPathResult result,
      List<RelationExecutionMetrics> metrics) {
    if (!enabled || prefix.isEmpty() || !cacheable(prefix)) {
      return;
    }
    entries.putIfAbsent(
        PrefixKey.of(sourceResource, prefix), new Entry(result, List.copyOf(metrics)));
  }

  private static boolean cacheable(List<RelationStep> relations) {
    // FilterExpression currently exposes dependencies but not a semantic fingerprint. Sharing a
    // filtered prefix based only on dependency columns could conflate predicates with different
    // literal values, so filtered prefixes remain deliberately one-off.
    return relations.stream().noneMatch(step -> step.filter().isPresent());
  }

  @Override
  public void close() {
    for (Entry entry : entries.values()) {
      if (entry.persisted) {
        entry.result.dataset().unpersist(false);
      }
    }
    entries.clear();
  }

  record Hit(int relationCount, SparkPathResult result, List<RelationExecutionMetrics> metrics) {}

  private static final class Entry {
    private final SparkPathResult result;
    private final List<RelationExecutionMetrics> metrics;
    private boolean persisted;

    private Entry(SparkPathResult result, List<RelationExecutionMetrics> metrics) {
      this.result = result;
      this.metrics = metrics;
    }
  }

  private record PrefixKey(String sourceResource, List<String> relations) {
    private static PrefixKey of(String sourceResource, List<RelationStep> steps) {
      List<String> fingerprints = new ArrayList<>(steps.size());
      for (RelationStep step : steps) {
        fingerprints.add(fingerprint(step));
      }
      return new PrefixKey(sourceResource, List.copyOf(fingerprints));
    }

    private static String fingerprint(RelationStep step) {
      String cardinality =
          step.cardinalityStrategy()
              .map(
                  strategy -> {
                    if (strategy instanceof CardinalityStrategy.Select select) {
                      return "SELECT:" + select.selector();
                    }
                    if (strategy instanceof CardinalityStrategy.Combine combine) {
                      return "COMBINE:" + combine.aggregation();
                    }
                    return strategy.getClass().getSimpleName();
                  })
              .orElse("EXACTLY_ONE");
      return String.join(
          "|",
          step.targetResource(),
          step.viaColumn().orElse(""),
          step.schemaPredicate().orElse(""),
          step.sourceColumn().orElse(""),
          step.targetColumn().orElse(""),
          cardinality,
          step.requirement().name());
    }
  }
}
