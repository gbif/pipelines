package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Executes a validated {@link SchemaPath} using schema-declared keys and path-qualified aliases.
 */
public final class SparkSchemaPathExecutor {
  private final SchemaGraph graph;

  public SparkSchemaPathExecutor(SchemaGraph graph) {
    this.graph = graph;
  }

  public SparkPathResult execute(TableLoader loader, SchemaPath path) {
    Dataset<Row> current = loadRequired(loader, path.rootResource());
    Map<FieldRef, String> aliases = new LinkedHashMap<>();
    current = aliasResource(current, SchemaPath.root(path.rootResource()), aliases);

    SchemaPath currentPath = SchemaPath.root(path.rootResource());
    for (SchemaRelation relation : path.relations()) {
      if (!relation.sourceResource().equals(currentPath.currentResource())) {
        throw new IllegalArgumentException("Broken schema path at " + relation);
      }
      if (!graph.hasColumn(relation.sourceResource(), relation.sourceColumn())
          || !graph.hasColumn(relation.targetResource(), relation.targetColumn())) {
        throw new IllegalArgumentException(
            "Schema relation references unavailable columns: " + relation);
      }

      Dataset<Row> targetRaw = loadRequired(loader, relation.targetResource());
      SchemaPath targetPath = currentPath.append(relation);
      Map<FieldRef, String> targetAliases = new LinkedHashMap<>();
      Dataset<Row> target = aliasResource(targetRaw, targetPath, targetAliases);

      String sourceAlias = aliases.get(currentPath.field(relation.sourceColumn()));
      String targetAlias = targetAliases.get(targetPath.field(relation.targetColumn()));
      if (sourceAlias == null) {
        throw new IllegalArgumentException(
            "Loaded dataset "
                + relation.sourceResource()
                + " is missing join column "
                + relation.sourceColumn());
      }
      if (targetAlias == null) {
        throw new IllegalArgumentException(
            "Loaded dataset "
                + relation.targetResource()
                + " is missing join column "
                + relation.targetColumn());
      }

      current =
          current.join(
              target, current.col(sourceAlias).equalTo(target.col(targetAlias)), "left_outer");
      aliases.putAll(targetAliases);
      currentPath = targetPath;
    }
    return new SparkPathResult(current, aliases);
  }

  private Dataset<Row> aliasResource(
      Dataset<Row> dataset, SchemaPath path, Map<FieldRef, String> aliases) {
    Column[] selected = new Column[dataset.columns().length];
    for (int i = 0; i < dataset.columns().length; i++) {
      String raw = dataset.columns()[i];
      FieldRef ref = path.field(raw);
      String alias = physicalAlias(ref);
      aliases.put(ref, alias);
      selected[i] = dataset.col(quote(raw)).as(alias);
    }
    return dataset.select(selected);
  }

  static String physicalAlias(FieldRef ref) {
    String readable = ref.qualifiedName().replaceAll("[^A-Za-z0-9]+", "_");
    return "__dwcdp__"
        + readable
        + "__"
        + Integer.toUnsignedString(ref.qualifiedName().hashCode(), 36);
  }

  private static Dataset<Row> loadRequired(TableLoader loader, String resource) {
    return loader
        .load(resource)
        .orElseThrow(
            () -> new IllegalArgumentException("Required path resource is absent: " + resource));
  }

  private static String quote(String column) {
    return "`" + column.replace("`", "``") + "`";
  }
}
