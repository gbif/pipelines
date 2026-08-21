package org.gbif.pipelines.spark.dwcdp.mapping.schema;

import java.util.Objects;

/** Field metadata loaded from an official DwC-DP table schema. */
public record SchemaField(String name, boolean required, boolean unique) {
  public SchemaField {
    Objects.requireNonNull(name, "name");
  }
}
