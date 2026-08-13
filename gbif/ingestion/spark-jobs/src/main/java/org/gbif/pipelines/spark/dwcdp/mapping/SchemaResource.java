package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Resource metadata loaded from an official DwC-DP table schema. */
public record SchemaResource(
    String name,
    Map<String, SchemaField> fields,
    Optional<String> primaryKey,
    Optional<String> weakPrimaryKey) {
  public SchemaResource {
    Objects.requireNonNull(name, "name");
    fields = Map.copyOf(fields);
    primaryKey = primaryKey == null ? Optional.empty() : primaryKey;
    weakPrimaryKey = weakPrimaryKey == null ? Optional.empty() : weakPrimaryKey;
  }
}
