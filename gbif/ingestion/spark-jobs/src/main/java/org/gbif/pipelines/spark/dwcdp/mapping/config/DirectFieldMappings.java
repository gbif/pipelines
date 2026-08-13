package org.gbif.pipelines.spark.dwcdp.mapping.config;

import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaResource;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

/**
 * Adds ordinary source fields as inferred target producers. The compiler, not this helper, resolves
 * precedence when an explicit mapping or a closer inferred mapping claims the same target.
 */
final class DirectFieldMappings {

  private final SchemaResource resource;
  private final SchemaPath path;

  private DirectFieldMappings(SchemaResource resource, SchemaPath path) {
    this.resource = resource;
    this.path = path;
  }

  static DirectFieldMappings from(SchemaGraph graph, String resourceName, SchemaPath path) {
    SchemaResource resource =
        graph.resource(resourceName)
            .orElseThrow(
                () -> new IllegalArgumentException("DwC-DP schema has no resource " + resourceName));
    return new DirectFieldMappings(resource, path);
  }

  void addTo(ExtensionFragmentBuilder builder) {
    for (String column : resource.fields().keySet()) {
      if (isStructural(column)) {
        continue;
      }
      builder.field(
          TargetFieldMapping.inferredOneOf(
              TargetTerms.resolve(column), ValueAggregation.firstNonNull(), path.field(column)));
    }
  }

  private static boolean isStructural(String column) {
    return column.endsWith("_pk") || column.endsWith("_fk");
  }
}
