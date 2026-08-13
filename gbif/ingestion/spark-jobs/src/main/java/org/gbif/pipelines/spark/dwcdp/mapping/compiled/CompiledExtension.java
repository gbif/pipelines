package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.List;
import java.util.Objects;

/** Compiled extension specification after target-producer precedence has been resolved. */
public record CompiledExtension(
    String rowType, List<CompiledFragment> fragments, List<MappingDecision> decisions) {
  public CompiledExtension {
    Objects.requireNonNull(rowType, "rowType");
    fragments = List.copyOf(fragments);
    decisions = List.copyOf(decisions);
  }
}
