package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.gbif.pipelines.spark.dwcdp.mapping.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageResource;

/** Physical resource/column availability derived from one dataset's datapackage.json. */
public record MappingDatasetScope(Map<String, Set<String>> columnsByResource) {

  public MappingDatasetScope {
    Objects.requireNonNull(columnsByResource, "columnsByResource");
    Map<String, Set<String>> copy = new LinkedHashMap<>();
    columnsByResource.forEach(
        (resource, columns) -> copy.put(resource, Set.copyOf(columns)));
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

  /** True when the full path and final source column physically exist in this dataset. */
  public boolean supports(FieldRef field) {
    if (!hasResource(field.path().rootResource())) {
      return false;
    }
    for (SchemaRelation relation : field.path().relations()) {
      if (!hasResource(relation.sourceResource())
          || !hasResource(relation.targetResource())
          || !hasColumn(relation.sourceResource(), relation.sourceColumn())
          || !hasColumn(relation.targetResource(), relation.targetColumn())) {
        return false;
      }
    }
    return hasColumn(field.path().currentResource(), field.column());
  }

  public boolean supports(CompiledSourceField source) {
    return supports(source.field());
  }

  /** A producer can contribute when at least one of its declared source alternatives is reachable. */
  public boolean supports(CompiledTargetProducer producer) {
    return producer.sources().stream().anyMatch(this::supports);
  }
}
