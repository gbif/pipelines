package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Inspectable compiler decision for one target term within one mapping scope. */
public record MappingDecision(
    String scope,
    String targetTerm,
    MappingDecisionType type,
    Optional<CompiledTargetProducer> selected,
    List<CompiledTargetProducer> candidates,
    String explanation) {

  public MappingDecision {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(targetTerm, "targetTerm");
    Objects.requireNonNull(type, "type");
    selected = selected == null ? Optional.empty() : selected;
    candidates = List.copyOf(candidates);
    Objects.requireNonNull(explanation, "explanation");
  }

  public boolean problem() {
    return type == MappingDecisionType.MISSING_FRAGMENT_SCOPE
        || type == MappingDecisionType.INVALID_RELATION
        || type == MappingDecisionType.AMBIGUOUS_MULTIPLE_EXPLICIT
        || type == MappingDecisionType.AMBIGUOUS_EQUAL_DEPTH;
  }

  public boolean ambiguous() {
    return type == MappingDecisionType.AMBIGUOUS_MULTIPLE_EXPLICIT
        || type == MappingDecisionType.AMBIGUOUS_EQUAL_DEPTH;
  }
}
