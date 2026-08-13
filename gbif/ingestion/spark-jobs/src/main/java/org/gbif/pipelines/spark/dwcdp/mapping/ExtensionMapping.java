package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.List;
import java.util.Objects;

/** One DwC-A extension row type assembled from one or more reusable fragments. */
public record ExtensionMapping(String rowType, List<ExtensionFragment> fragments) {
  public ExtensionMapping {
    Objects.requireNonNull(rowType, "rowType");
    fragments = List.copyOf(fragments);
    if (fragments.stream().anyMatch(f -> !rowType.equals(f.rowType()))) {
      throw new IllegalArgumentException("All imported fragments must target extension " + rowType);
    }
  }
}
