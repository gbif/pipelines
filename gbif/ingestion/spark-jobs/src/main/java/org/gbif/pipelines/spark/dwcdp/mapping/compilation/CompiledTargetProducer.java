package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetValue;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression;

/** One compiled producer of a target term with complete logical source lineage. */
public record CompiledTargetProducer(
    String targetTerm,
    String owner,
    TargetValue value,
    List<CompiledSourceField> sources,
    TargetFieldMapping.Origin origin,
    Optional<CompiledSourceField> contributionIdentity,
    Optional<CompiledSourceField> orderBy) {

  public CompiledTargetProducer(
      String targetTerm,
      String owner,
      TargetFieldMapping.SourceMode sourceMode,
      ValueAggregation aggregation,
      List<CompiledSourceField> sources,
      TargetFieldMapping.Origin origin,
      Optional<CompiledSourceField> contributionIdentity,
      Optional<CompiledSourceField> orderBy) {
    this(
        targetTerm,
        owner,
        new TargetValue.Aggregated(
            sourceMode, sources.stream().map(CompiledSourceField::field).toList(), aggregation),
        sources,
        origin,
        contributionIdentity,
        orderBy);
  }

  public CompiledTargetProducer {
    Objects.requireNonNull(targetTerm, "targetTerm");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(value, "value");
    sources = List.copyOf(sources);
    Objects.requireNonNull(origin, "origin");
    contributionIdentity = contributionIdentity == null ? Optional.empty() : contributionIdentity;
    orderBy = orderBy == null ? Optional.empty() : orderBy;
    if (value instanceof TargetValue.Aggregated && sources.isEmpty()) {
      throw new IllegalArgumentException("Compiled aggregated target producer requires a source");
    }
  }

  public boolean expressionValue() {
    return value instanceof TargetValue.Expression;
  }

  public TargetFieldMapping.SourceMode sourceMode() {
    return aggregatedValue().sourceMode();
  }

  public ValueAggregation aggregation() {
    return aggregatedValue().aggregation();
  }

  public ValueExpression expression() {
    if (value instanceof TargetValue.Expression expressionValue) {
      return expressionValue.expression();
    }
    throw new IllegalStateException(
        "Target producer is aggregation-backed, not expression-backed: target="
            + targetTerm
            + ", owner="
            + owner
            + ", value="
            + value
            + ". expression() is only valid for TargetValue.Expression producers.");
  }

  private TargetValue.Aggregated aggregatedValue() {
    if (value instanceof TargetValue.Aggregated aggregated) {
      return aggregated;
    }
    throw new IllegalStateException(
        "Target producer is expression-backed, not aggregation-backed: target="
            + targetTerm
            + ", owner="
            + owner
            + ", value="
            + value
            + ". sourceMode() and aggregation() are only valid for TargetValue.Aggregated producers. "
            + "Evaluate expression() row-wise and obtain any enclosing reduction from the "
            + "materialization context (for example a CompiledTargetMerge).");
  }

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
    out.append("strategy: ").append(value).append('\n');
    if (origin == TargetFieldMapping.Origin.INFERRED) {
      out.append("inferred path depth: ").append(pathDepth()).append('\n');
    }
    contributionIdentity.ifPresent(
        source -> out.append("contribution identity: ").append(source.describe()).append('\n'));
    orderBy.ifPresent(source -> out.append("order by: ").append(source.describe()).append('\n'));
    out.append("sources:");
    for (CompiledSourceField source : sources) {
      out.append("\n  - ").append(source.describe());
    }
    return out.toString();
  }
}
