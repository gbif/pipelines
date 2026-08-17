package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingInputRequirements;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingInputRequirements.ResourceRequirement;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Plan-aware {@link TableLoader} wrapper that lets Catalyst/Parquet read only columns required by
 * the compiled mapping and hides resources unused by that plan.
 */
public final class ProjectedTableLoader {
  private ProjectedTableLoader() {}

  public static TableLoader wrap(TableLoader delegate, MappingInputRequirements requirements) {
    Objects.requireNonNull(delegate, "delegate");
    Objects.requireNonNull(requirements, "requirements");

    return resource -> {
      if (!requirements.usesResource(resource)) {
        return Optional.empty();
      }
      Optional<Dataset<Row>> loaded = delegate.load(resource);
      if (loaded.isEmpty()) {
        return loaded;
      }

      ResourceRequirement requirement = requirements.resource(resource);
      Dataset<Row> dataset = loaded.get();
      if (requirement.allColumns() || requirement.columns().isEmpty()) {
        return Optional.of(dataset);
      }

      Set<String> required = new HashSet<>(requirement.columns());
      List<Column> selected = new ArrayList<>();
      for (String physical : dataset.columns()) {
        if (required.contains(physical)) {
          selected.add(dataset.col(physical));
        }
      }
      return Optional.of(dataset.select(selected.toArray(Column[]::new)));
    };
  }
}
