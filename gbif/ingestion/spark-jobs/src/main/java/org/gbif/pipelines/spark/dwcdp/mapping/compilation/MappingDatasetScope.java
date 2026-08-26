package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageResource;

/** Physical resource/column availability derived from one dataset's datapackage.json. */
public record MappingDatasetScope(Map<String, Set<String>> columnsByResource) {

  public MappingDatasetScope {
    Objects.requireNonNull(columnsByResource, "columnsByResource");
    Map<String, Set<String>> copy = new LinkedHashMap<>();
    columnsByResource.forEach((resource, columns) -> copy.put(resource, Set.copyOf(columns)));
    columnsByResource = Map.copyOf(copy);
  }

  public static MappingDatasetScope from(DataPackage dataPackage) {
    Objects.requireNonNull(dataPackage, "dataPackage");
    Map<String, Set<String>> resources = new LinkedHashMap<>();
    for (DataPackageResource resource : dataPackage.getResources()) {
      Set<String> fields = new LinkedHashSet<>();
      if (resource.getSchema() != null && resource.getSchema().getFields() != null) {
        resource.getSchema().getFields().forEach(field -> fields.add(field.getName()));
      }
      resources.put(resource.getName(), Set.copyOf(fields));
    }
    return new MappingDatasetScope(resources);
  }

  public boolean hasResource(String resource) {
    return columnsByResource.containsKey(resource);
  }

  public boolean hasColumn(String resource, String column) {
    return columnsByResource.getOrDefault(resource, Set.of()).contains(column);
  }

  /**
   * True when this complete physical field path is declared by the datapackage. Dataset-scoped
   * compilation treats the descriptor as the executable contract: missing fields and join keys are
   * removed before Spark planning rather than discovered as unresolved columns at runtime.
   */
  public boolean supports(FieldRef field) {
    if (!hasResource(field.path().rootResource())) {
      return false;
    }
    for (SchemaRelation relation : field.path().relations()) {
      if (!hasResource(relation.sourceResource())
          || !hasColumn(relation.sourceResource(), relation.sourceColumn())
          || !hasResource(relation.targetResource())
          || !hasColumn(relation.targetResource(), relation.targetColumn())) {
        return false;
      }
    }
    return hasColumn(field.path().currentResource(), field.column());
  }

  public boolean supports(CompiledSourceField source) {
    return supports(source.field());
  }

  /** True when every physical dependency needed to execute one relation is declared. */
  public boolean supports(CompiledRelationStep relationStep) {
    SchemaRelation relation = relationStep.relation();
    if (!hasResource(relation.sourceResource())
        || !hasColumn(relation.sourceResource(), relation.sourceColumn())
        || !hasResource(relation.targetResource())
        || !hasColumn(relation.targetResource(), relation.targetColumn())) {
      return false;
    }

    if (relationStep.cardinalityStrategy().isPresent()
        && relationStep.cardinalityStrategy().orElseThrow()
            instanceof
            org.gbif.pipelines.spark.dwcdp.mapping.definition.CardinalityStrategy.Select
            select
        && !hasColumn(relation.targetResource(), select.selector())) {
      return false;
    }

    if (relationStep.filter().isPresent()) {
      if (relationStep.filter().requiresAllColumns()) {
        // Opaque predicates do not expose enough dependency information to prove that a
        // dataset-scoped Spark plan is safe. Do not execute them in a specialized plan.
        return false;
      }
      return relationStep.filter().requiredColumns().stream()
          .allMatch(column -> hasColumn(relation.targetResource(), column));
    }
    return true;
  }

  /**
   * A producer can contribute when at least one of its declared source alternatives is reachable.
   */
  public boolean supports(CompiledTargetProducer producer) {
    return producer.sources().stream().anyMatch(this::supports);
  }
}
