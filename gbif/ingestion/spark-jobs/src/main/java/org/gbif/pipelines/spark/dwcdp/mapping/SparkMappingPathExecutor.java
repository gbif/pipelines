package org.gbif.pipelines.spark.dwcdp.mapping;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.when;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Executes a {@link Mapping}'s DwC-DP navigation semantics.
 *
 * <p>This layer deliberately stops before DwC-A target materialization. It owns schema-resolved joins,
 * row filters, relation cardinality, optional resources, qualified field aliases, and funnel metrics.
 */
public final class SparkMappingPathExecutor {
  private static final String INTERNAL_PARENT_ID = "__dwcdp_parent_row_id";
  private static final String INTERNAL_MATCH_COUNT = "__dwcdp_match_count";
  private static final String INTERNAL_ROW_NUMBER = "__dwcdp_row_number";

  private final SchemaGraph graph;

  public SparkMappingPathExecutor(SchemaGraph graph) {
    this.graph = graph;
  }

  public MappingExecutionResult execute(TableLoader loader, Mapping mapping) {
    ValidationResult validation = MappingValidator.validate(mapping, graph);
    if (!validation.isValid()) {
      throw new IllegalArgumentException("Invalid mapping " + mapping.name() + ": " + validation.issues());
    }

    Dataset<Row> current = loadRequired(loader, mapping.sourceResource());
    Map<FieldRef, String> aliases = new LinkedHashMap<>();
    SchemaPath currentPath = SchemaPath.root(mapping.sourceResource());
    current = aliasResource(current, currentPath, aliases);

    List<RelationExecutionMetrics> metrics = new ArrayList<>();

    for (RelationStep step : mapping.relations()) {
      SchemaRelation relation =
          graph.resolve(
              currentPath.currentResource(),
              step.targetResource(),
              step.viaColumn().orElse(null),
              step.schemaPredicate().orElse(null));

      Optional<Dataset<Row>> targetRawOpt = loader.load(relation.targetResource());
      long inputRows = current.count();
      SchemaPath targetPath = currentPath.append(relation);
      if (targetRawOpt.isEmpty()) {
        if (step.requirement() == RelationRequirement.REQUIRED) {
          throw new IllegalArgumentException(
              "Required path resource is absent: " + relation.targetResource());
        }
        current = addNullResource(current, targetPath, relation.targetResource(), aliases);
        metrics.add(
            RelationExecutionMetrics.skipped(
                relation.sourceResource(), relation.targetResource(), inputRows));
        currentPath = targetPath;
        continue;
      }

      String sourceAlias = aliases.get(currentPath.field(relation.sourceColumn()));
      if (sourceAlias == null) {
        if (step.requirement() == RelationRequirement.REQUIRED) {
          throw new IllegalArgumentException(
              "Loaded dataset "
                  + relation.sourceResource()
                  + " is missing required join column "
                  + relation.sourceColumn());
        }
        current = addNullResource(current, targetPath, relation.targetResource(), aliases);
        metrics.add(
            RelationExecutionMetrics.skipped(
                relation.sourceResource(), relation.targetResource(), inputRows));
        currentPath = targetPath;
        continue;
      }
      long sourceKeyPresentRows = current.filter(col(sourceAlias).isNotNull()).count();

      Dataset<Row> targetRaw = targetRawOpt.get();
      if (!hasColumn(targetRaw, relation.targetColumn())) {
        if (step.requirement() == RelationRequirement.REQUIRED) {
          throw new IllegalArgumentException(
              "Required target join column is absent: "
                  + relation.targetResource()
                  + "."
                  + relation.targetColumn());
        }
        current = addNullResource(current, targetPath, relation.targetResource(), aliases);
        metrics.add(
            RelationExecutionMetrics.skipped(
                relation.sourceResource(), relation.targetResource(), inputRows));
        currentPath = targetPath;
        continue;
      }

      long targetRowsBeforeFilter = targetRaw.count();
      Dataset<Row> filteredTarget = applyFilter(targetRaw, step.filter());
      long targetRowsAfterFilter = filteredTarget.count();

      Map<FieldRef, String> targetAliases = new LinkedHashMap<>();
      Dataset<Row> target = aliasResource(filteredTarget, targetPath, targetAliases);
      String targetAlias = targetAliases.get(targetPath.field(relation.targetColumn()));

      Dataset<Row> parent = current.withColumn(INTERNAL_PARENT_ID, monotonically_increasing_id());
      Dataset<Row> joined =
          parent.join(target, parent.col(sourceAlias).equalTo(target.col(targetAlias)), "left_outer");

      WindowSpec parentWindow = Window.partitionBy(col(INTERNAL_PARENT_ID));
      joined = joined.withColumn(INTERNAL_MATCH_COUNT, count(col(targetAlias)).over(parentWindow));

      long matchedParentRows =
          joined.filter(col(INTERNAL_MATCH_COUNT).gt(0))
              .select(INTERNAL_PARENT_ID)
              .distinct()
              .count();
      long unmatchedParentRows = inputRows - matchedParentRows;
      long multipleMatchParentRows =
          joined.filter(col(INTERNAL_MATCH_COUNT).gt(1))
              .select(INTERNAL_PARENT_ID)
              .distinct()
              .count();

      CardinalityStrategy strategy =
          step.cardinalityStrategy().orElseGet(CardinalityStrategy::exactlyOne);
      joined = applyCardinality(joined, targetAliases, targetPath, strategy, parentWindow);
      joined = joined.drop(INTERNAL_PARENT_ID).drop(INTERNAL_MATCH_COUNT).drop(INTERNAL_ROW_NUMBER);

      long outputRows = joined.count();
      metrics.add(
          new RelationExecutionMetrics(
              relation.sourceResource(),
              relation.targetResource(),
              inputRows,
              sourceKeyPresentRows,
              targetRowsBeforeFilter,
              targetRowsAfterFilter,
              matchedParentRows,
              unmatchedParentRows,
              multipleMatchParentRows,
              outputRows,
              false));

      current = joined;
      aliases.putAll(targetAliases);
      currentPath = targetPath;
    }

    return new MappingExecutionResult(new SparkPathResult(current, aliases), metrics, true);
  }

  private Dataset<Row> applyCardinality(
      Dataset<Row> joined,
      Map<FieldRef, String> targetAliases,
      SchemaPath targetPath,
      CardinalityStrategy strategy,
      WindowSpec parentWindow) {
    if (strategy instanceof CardinalityStrategy.FanOut) {
      return joined;
    }

    if (strategy instanceof CardinalityStrategy.ExactlyOne) {
      Dataset<Row> out = joined;
      for (String alias : targetAliases.values()) {
        out = out.withColumn(alias, when(col(INTERNAL_MATCH_COUNT).equalTo(1L), col(alias)).otherwise(lit(null)));
      }
      return out.dropDuplicates(INTERNAL_PARENT_ID);
    }

    if (strategy instanceof CardinalityStrategy.Select select) {
      FieldRef selectorRef = targetPath.field(select.selector());
      String selectorAlias = targetAliases.get(selectorRef);
      if (selectorAlias == null) {
        throw new IllegalArgumentException(
            "Selector field is not present on target path "
                + targetPath.currentResource()
                + ": "
                + select.selector());
      }
      Dataset<Row> ranked =
          joined.withColumn(
              INTERNAL_ROW_NUMBER,
              row_number().over(parentWindow.orderBy(col(selectorAlias).asc_nulls_last())));
      return ranked.filter(col(INTERNAL_ROW_NUMBER).equalTo(1));
    }

    if (strategy instanceof CardinalityStrategy.Combine) {
      throw new UnsupportedOperationException(
          "Relation-level combine is intentionally deferred to target/extension materialization; "
              + "combining every target column independently would destroy row correlation");
    }

    throw new IllegalArgumentException("Unsupported cardinality strategy: " + strategy);
  }


  private Dataset<Row> addNullResource(
      Dataset<Row> dataset, SchemaPath path, String resource, Map<FieldRef, String> aliases) {
    SchemaResource schemaResource =
        graph.resource(resource)
            .orElseThrow(() -> new IllegalArgumentException("Unknown schema resource: " + resource));
    Dataset<Row> out = dataset;
    for (String raw : schemaResource.fields().keySet()) {
      FieldRef ref = path.field(raw);
      String alias = SparkSchemaPathExecutor.physicalAlias(ref);
      aliases.put(ref, alias);
      if (!hasColumn(out, alias)) {
        out = out.withColumn(alias, lit(null));
      }
    }
    return out;
  }

  private static Dataset<Row> applyFilter(Dataset<Row> target, FilterExpression filter) {
    return filter.isPresent() ? target.filter(filter.build(FieldColumns.of(target))) : target;
  }

  private Dataset<Row> aliasResource(
      Dataset<Row> dataset, SchemaPath path, Map<FieldRef, String> aliases) {
    Column[] selected = new Column[dataset.columns().length];
    for (int i = 0; i < dataset.columns().length; i++) {
      String raw = dataset.columns()[i];
      FieldRef ref = path.field(raw);
      String alias = SparkSchemaPathExecutor.physicalAlias(ref);
      aliases.put(ref, alias);
      selected[i] = dataset.col(quote(raw)).as(alias);
    }
    return dataset.select(selected);
  }

  private static Dataset<Row> loadRequired(TableLoader loader, String resource) {
    return loader
        .load(resource)
        .orElseThrow(() -> new IllegalArgumentException("Required root resource is absent: " + resource));
  }

  private static boolean hasColumn(Dataset<Row> dataset, String column) {
    for (String candidate : dataset.columns()) {
      if (candidate.equals(column)) {
        return true;
      }
    }
    return false;
  }

  private static String quote(String column) {
    return "`" + column.replace("`", "``") + "`";
  }
}
