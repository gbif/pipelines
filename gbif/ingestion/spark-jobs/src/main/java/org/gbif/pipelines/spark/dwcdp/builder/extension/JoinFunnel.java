package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.List;

/**
 * Generic candidates-to-buckets breakdown for a join/enrichment builder (e.g. {@link
 * AgentJoinBuilder}, {@link ProtocolJoinBuilder}, {@link GeologicalContextJoinBuilder}) — how many
 * candidate rows the join considered, and how each ended up bucketed (resolved, ambiguous,
 * unresolved, table absent, ...).
 *
 * <p>{@link MaterialJoinBuilder.MaterialFunnel} predates this type and keeps its own bespoke record
 * shape (it's also consumed directly by {@code virtualMaterialOccurrences} for actual routing
 * decisions, not just reporting) — every other join builder's funnel maps onto this shared shape
 * instead, so {@code DwcDpVerbatimConverter#writeConversionReport} can render an arbitrary number
 * of them via a single loop over {@code List<JoinFunnel>} rather than needing a new named parameter
 * and a new render block per builder.
 *
 * <p>Buckets are conventionally ordered candidates-first and should sum to the candidate count
 * where that's meaningful, but this type itself doesn't enforce that — each builder's {@code
 * computeFunnel} is responsible for its own bucket semantics, same as {@code MaterialFunnel} today.
 */
public record JoinFunnel(String label, List<Bucket> buckets) {

  public record Bucket(String name, long count) {}
}
