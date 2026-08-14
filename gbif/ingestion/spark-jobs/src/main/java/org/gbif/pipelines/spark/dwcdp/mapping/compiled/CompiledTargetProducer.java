package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

/**
 * One producer of a target term, including ownership, origin and complete source lineage.
 * Spark physical aliases are deliberately not part of this representation.
 */
public record CompiledTargetProducer(
    String targetTerm,
    String owner,
    TargetFieldMapping.SourceMode sourceMode,
    ValueAggregation aggregation,
    List<CompiledSourceField> sources,
    TargetFieldMapping.Origin origin,
    Optional<CompiledSourceField> contributionIdentity,
    Optional<CompiledSourceField> orderBy) {

  public CompiledTargetProducer {
    Objects.requireNonNull(targetTerm, "targetTerm");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(sourceMode, "sourceMode");
    Objects.requireNonNull(aggregation, "aggregation");
    sources = List.copyOf(sources);
    Objects.requireNonNull(origin, "origin");
    contributionIdentity = contributionIdentity == null ? Optional.empty() : contributionIdentity;
    orderBy = orderBy == null ? Optional.empty() : orderBy;
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("Compiled target producer requires at least one source");
    }
  }

  /**
   * Distance used only for inferred-producer precedence. A producer's closest source determines its
   * rank; explicit producers ignore this value because they outrank inferred producers entirely.
   */
  public int pathDepth() {
    return sources.stream()
        .mapToInt(source -> source.field().path().relations().size())
        .min()
        .orElse(Integer.MAX_VALUE);
  }

  public String describe() {
    StringBuilder out = new StringBuilder();
    out.append("owner: ").append(owner).append('\n');
    out.append("origin: ").append(origin).append('\n');
    out.append("strategy: ").append(sourceMode).append(" / ").append(aggregation).append('\n');
    if (origin == TargetFieldMapping.Origin.INFERRED) {
      out.append("inferred path depth: ").append(pathDepth()).append('\n');
    }
    contributionIdentity.ifPresent(source -> out.append("contribution identity: ").append(source.describe()).append('\n'));
    orderBy.ifPresent(source -> out.append("order by: ").append(source.describe()).append('\n'));
    out.append("sources:");
    for (CompiledSourceField source : sources) {
      out.append("\n  - ").append(source.describe());
    }
    return out.toString();
  }
}
