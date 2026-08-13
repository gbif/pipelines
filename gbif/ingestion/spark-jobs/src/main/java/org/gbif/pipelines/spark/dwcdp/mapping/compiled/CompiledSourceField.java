package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.FieldRef;

/** Engine-neutral source-field lineage retained after mapping compilation. */
public record CompiledSourceField(FieldRef field) {
  public CompiledSourceField {
    Objects.requireNonNull(field, "field");
  }

  public String describe() {
    return field.qualifiedName();
  }
}
