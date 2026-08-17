package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Physical DwC-DP input dependencies derived from one compiled mapping plan.
 *
 * <p>These requirements are intentionally expressed per resource rather than per fragment because
 * a {@code TableLoader} is shared across all branches. The union is therefore the smallest safe
 * projection for the complete compiled plan.
 */
public record MappingInputRequirements(Map<String, ResourceRequirement> resources) {

  public MappingInputRequirements {
    Objects.requireNonNull(resources, "resources");
    resources = Map.copyOf(resources);
  }

  public boolean usesResource(String resource) {
    return resources.containsKey(resource);
  }

  public ResourceRequirement resource(String resource) {
    return resources.get(resource);
  }

  /** One resource's projected columns. {@code allColumns=true} is the conservative escape hatch. */
  public record ResourceRequirement(Set<String> columns, boolean allColumns) {
    public ResourceRequirement {
      Objects.requireNonNull(columns, "columns");
      columns = Set.copyOf(columns);
    }
  }

  static final class Builder {
    private final Map<String, MutableRequirement> resources = new LinkedHashMap<>();

    void use(String resource) {
      resources.computeIfAbsent(resource, ignored -> new MutableRequirement());
    }

    void column(String resource, String column) {
      use(resource);
      if (column != null && !column.isBlank()) {
        resources.get(resource).columns.add(column);
      }
    }

    void allColumns(String resource) {
      use(resource);
      resources.get(resource).allColumns = true;
    }

    MappingInputRequirements build() {
      Map<String, ResourceRequirement> result = new LinkedHashMap<>();
      resources.forEach(
          (name, requirement) ->
              result.put(
                  name,
                  new ResourceRequirement(
                      new LinkedHashSet<>(requirement.columns), requirement.allColumns)));
      return new MappingInputRequirements(result);
    }

    private static final class MutableRequirement {
      private final Set<String> columns = new LinkedHashSet<>();
      private boolean allColumns;
    }
  }
}
