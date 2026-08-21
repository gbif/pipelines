package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.Objects;
import java.util.stream.Collectors;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;

/** Path-qualified logical field identity. Spark aliases are an executor concern. */
public record FieldRef(SchemaPath path, String column) {
  public FieldRef {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(column, "column");
  }

  public String qualifiedName() {
    String suffix =
        path.relations().stream()
            .map(
                r ->
                    r.predicate().map(p -> p + ":" + r.targetResource()).orElse(r.targetResource()))
            .collect(Collectors.joining("/"));
    return path.rootResource() + (suffix.isEmpty() ? "" : "/" + suffix) + "." + column;
  }
}
