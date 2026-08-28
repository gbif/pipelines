package org.gbif.pipelines.spark.dwcdp.mapping.config;

import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;

/** One independent Event -> Occurrence ownership discovery path. */
public record EventOccurrenceDiscoveryFragment(
    String name,
    MappingPath path,
    FieldRef event,
    FieldRef occurrence,
    Optional<FieldRef> material) {

  public EventOccurrenceDiscoveryFragment {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(event, "event");
    Objects.requireNonNull(occurrence, "occurrence");
    material = material == null ? Optional.empty() : material;
  }
}
