package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;

/** Engine-neutral source-field lineage retained after mapping compilation. */
public record CompiledSourceField(FieldRef field) {
  public CompiledSourceField {
    Objects.requireNonNull(field, "field");
  }

  public String describe() {
    return field.qualifiedName();
  }
}
