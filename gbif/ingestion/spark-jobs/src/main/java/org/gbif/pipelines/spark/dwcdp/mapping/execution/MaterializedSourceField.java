package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledSourceField;

/** Source lineage plus its physical Spark alias for one concrete path execution. */
public record MaterializedSourceField(CompiledSourceField source, Optional<String> physicalColumn) {
  public MaterializedSourceField {
    Objects.requireNonNull(source, "source");
    physicalColumn = physicalColumn == null ? Optional.empty() : physicalColumn;
  }

  public String describe() {
    return source.describe()
        + physicalColumn.map(column -> " [spark=" + column + "]").orElse(" [spark=<missing/null>]");
  }
}
