package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionRowComposition;

/** Compiled extension specification after target-producer precedence has been resolved. */
public record CompiledExtension(
    String rowType,
    ExtensionRowComposition rowComposition,
    Optional<Integer> maxRowsPerParent,
    List<CompiledTargetMerge> targetMerges,
    List<CompiledFragment> fragments,
    List<MappingDecision> decisions) {
  public CompiledExtension {
    Objects.requireNonNull(rowType, "rowType");
    Objects.requireNonNull(rowComposition, "rowComposition");
    maxRowsPerParent = Objects.requireNonNull(maxRowsPerParent, "maxRowsPerParent");
    targetMerges = List.copyOf(targetMerges);
    fragments = List.copyOf(fragments);
    decisions = List.copyOf(decisions);
  }
}
