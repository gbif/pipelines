package org.gbif.pipelines.spark.dwcdp.mapping.config;

import java.util.Set;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlanBuilder;
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
  private final Set<String> retainedRawOutputs;
  private final boolean humboldtRawContract;

  private DirectFieldMappings(
      SchemaResource resource,
      SchemaPath path,
      Set<String> retainedRawOutputs,
      boolean humboldtRawContract) {
    this.resource = resource;
    this.path = path;
    this.retainedRawOutputs = retainedRawOutputs;
    this.humboldtRawContract = humboldtRawContract;
  }

  static DirectFieldMappings from(SchemaGraph graph, String resourceName, SchemaPath path) {
    SchemaResource resource =
        graph.resource(resourceName)
            .orElseThrow(
                () -> new IllegalArgumentException("DwC-DP schema has no resource " + resourceName));
    return new DirectFieldMappings(resource, path, Set.of(), false);
  }

  static DirectFieldMappings from(
      SchemaGraph graph, String resourceName, SchemaPath path, Set<String> retainedRawOutputs) {
    SchemaResource resource =
        graph.resource(resourceName)
            .orElseThrow(
                () -> new IllegalArgumentException("DwC-DP schema has no resource " + resourceName));
    return new DirectFieldMappings(resource, path, Set.copyOf(retainedRawOutputs), false);
  }

  static DirectFieldMappings humboldt(
      SchemaGraph graph, String resourceName, SchemaPath path) {
    SchemaResource resource =
        graph.resource(resourceName)
            .orElseThrow(
                () -> new IllegalArgumentException("DwC-DP schema has no resource " + resourceName));
    return new DirectFieldMappings(resource, path, Set.of(), true);
  }

  void addTo(ExtensionFragmentBuilder builder) {
    for (String column : resource.fields().keySet()) {
      if (isStructural(column)) {
        continue;
      }
      resolveOutput(column)
          .ifPresent(
              target ->
                  builder.field(
                      TargetFieldMapping.inferredOneOf(
                          target, ValueAggregation.firstNonNull(), path.field(column))));
    }
  }

  void addTo(CoreFragmentBuilder builder) {
    for (String column : resource.fields().keySet()) {
      if (isStructural(column)) {
        continue;
      }
      resolveOutput(column)
          .ifPresent(
              target ->
                  builder.field(
                      TargetFieldMapping.inferredOneOf(
                          target, ValueAggregation.firstNonNull(), path.field(column))));
    }
  }

  void addTo(MappingPlanBuilder builder) {
    for (String column : resource.fields().keySet()) {
      if (isStructural(column)) {
        continue;
      }
      resolveOutput(column)
          .ifPresent(
              target ->
                  builder.coreField(
                      TargetFieldMapping.inferredOneOf(
                          target, ValueAggregation.firstNonNull(), path.field(column))));
    }
  }

  private java.util.Optional<String> resolveOutput(String column) {
    return humboldtRawContract
        ? TargetTerms.resolveHumboldtOutput(column)
        : TargetTerms.resolveOutput(column, retainedRawOutputs);
  }

  private static boolean isStructural(String column) {
    return column.endsWith("_pk") || column.endsWith("_fk");
  }
}
