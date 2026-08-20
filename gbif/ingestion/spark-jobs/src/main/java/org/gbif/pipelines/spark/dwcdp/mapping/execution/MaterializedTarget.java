package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.util.List;
import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledTargetProducer;

/** A compiled target producer after binding its logical source fields to Spark physical aliases. */
public record MaterializedTarget(
    CompiledTargetProducer producer, String physicalColumn, List<MaterializedSourceField> sources) {

  public MaterializedTarget {
    Objects.requireNonNull(producer, "producer");
    Objects.requireNonNull(physicalColumn, "physicalColumn");
    sources = List.copyOf(sources);
  }

  public String targetTerm() {
    return producer.targetTerm();
  }

  public String describe() {
    StringBuilder out = new StringBuilder();
    out.append("fragment: ").append(producer.owner()).append('\n');
    out.append("strategy: ")
        .append(producer.sourceMode())
        .append(" / ")
        .append(producer.aggregation())
        .append('\n');
    out.append("target spark column: ").append(physicalColumn).append('\n');
    out.append("sources:");
    for (MaterializedSourceField source : sources) {
      out.append("\n  - ").append(source.describe());
    }
    return out.toString();
  }
}
