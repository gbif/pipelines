package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Objects;

/** A field exposed by a resource participating in the mapping path. */
public record FieldSource(String resource, String column) {
  public FieldSource {
    Objects.requireNonNull(resource, "resource");
    Objects.requireNonNull(column, "column");
  }

  public static FieldSource field(String resource, String column) {
    return new FieldSource(resource, column);
  }
}
