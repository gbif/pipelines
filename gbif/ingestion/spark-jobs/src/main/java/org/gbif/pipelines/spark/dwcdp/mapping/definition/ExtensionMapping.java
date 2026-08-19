package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One DwC-A extension row type assembled from one or more reusable fragments. */
public record ExtensionMapping(
    String rowType,
    ExtensionRowComposition rowComposition,
    Optional<Integer> maxRowsPerParent,
    List<TargetMerge> targetMerges,
    List<ExtensionFragment> fragments) {

  public ExtensionMapping(String rowType, List<ExtensionFragment> fragments) {
    this(rowType, ExtensionRowComposition.ENRICH, Optional.empty(), List.of(), fragments);
  }

  public ExtensionMapping(
      String rowType, ExtensionRowComposition rowComposition, List<ExtensionFragment> fragments) {
    this(rowType, rowComposition, Optional.empty(), List.of(), fragments);
  }

  public ExtensionMapping {
    Objects.requireNonNull(rowType, "rowType");
    Objects.requireNonNull(rowComposition, "rowComposition");
    Objects.requireNonNull(maxRowsPerParent, "maxRowsPerParent");
    targetMerges = List.copyOf(targetMerges);
    maxRowsPerParent.ifPresent(
        limit -> {
          if (limit <= 0) {
            throw new IllegalArgumentException("maxRowsPerParent must be > 0");
          }
        });
    fragments = List.copyOf(fragments);
    if (fragments.stream().anyMatch(f -> !rowType.equals(f.rowType()))) {
      throw new IllegalArgumentException("All imported fragments must target extension " + rowType);
    }
  }
}
