package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sum;
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
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingValidator;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.ValidationResult;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CardinalityStrategy;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.Mapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationRequirement;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationStep;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelationResolver;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaResource;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Executes a {@link Mapping}'s DwC-DP navigation semantics.
 *
 * <p>This layer deliberately stops before DwC-A target materialization. It owns schema-resolved
 * joins, row filters, relation cardinality, optional resources, qualified field aliases, and funnel
 * metrics.
 */
public final class SparkMappingPathExecutor {
  private static final String INTERNAL_PARENT_ID = "__dwcdp_parent_row_id";
  private static final String INTERNAL_MATCH_COUNT = "__dwcdp_match_count";
  private static final String INTERNAL_ROW_NUMBER = "__dwcdp_row_number";

  private final SchemaGraph graph;
  private final ExecutionMetricsCollector metricsCollector;
  private final SparkPathPrefixCache prefixCache;

  public SparkMappingPathExecutor(SchemaGraph graph) {
    this(graph, new ExecutionMetricsCollector(), SparkPathPrefixCache.disabled());
  }

  SparkMappingPathExecutor(SchemaGraph graph, ExecutionMetricsCollector metricsCollector) {
    this(graph, metricsCollector, SparkPathPrefixCache.disabled());
  }

  SparkMappingPathExecutor(
      SchemaGraph graph,
      ExecutionMetricsCollector metricsCollector,
      SparkPathPrefixCache prefixCache) {
    this.graph = graph;
    this.metricsCollector = metricsCollector;
    this.prefixCache = prefixCache;
  }

  public MappingExecutionResult execute(TableLoader loader, Mapping mapping) {
    ValidationResult validation = MappingValidator.validate(mapping, graph);
    if (!validation.isValid()) {
      throw new IllegalArgumentException(
          "Invalid mapping " + mapping.name() + ": " + validation.issues());
    }

    Dataset<Row> current = loadRequired(loader, mapping.sourceResource());
    Map<FieldRef, String> aliases = new LinkedHashMap<>();
    SchemaPath currentPath = SchemaPath.root(mapping.sourceResource());
    current = aliasResource(current, currentPath, aliases);

    List<RelationExecutionMetrics> metrics = new ArrayList<>();
    int startRelation = 0;
    Optional<SparkPathPrefixCache.Hit> cached =
        prefixCache.longest(mapping.sourceResource(), mapping.relations());
    if (cached.isPresent()) {
      SparkPathPrefixCache.Hit hit = cached.get();
      current = hit.result().dataset();
      aliases.clear();
      aliases.putAll(hit.result().aliases());
      metrics.addAll(hit.metrics());
      currentPath = pathAfter(mapping.sourceResource(), mapping.relations(), hit.relationCount());
      startRelation = hit.relationCount();
    }

    for (int relationIndex = startRelation;
        relationIndex < mapping.relations().size();
        relationIndex++) {
      RelationStep step = mapping.relations().get(relationIndex);
      SchemaRelation relation =
          SchemaRelationResolver.resolve(graph, currentPath.currentResource(), step);

      Optional<Dataset<Row>> targetRawOpt = loader.load(relation.targetResource());
      SchemaPath targetPath = currentPath.append(relation);
      if (targetRawOpt.isEmpty()) {
        long inputRows = current.count();
        if (step.requirement() == RelationRequirement.REQUIRED) {
          throw new IllegalArgumentException(
              "Required path resource is absent: " + relation.targetResource());
        }
        current = addNullResource(current, targetPath, relation.targetResource(), aliases);
        metrics.add(
            RelationExecutionMetrics.skipped(
                relation.sourceResource(), relation.targetResource(), step, inputRows));
        currentPath = targetPath;
        rememberPrefix(mapping, relationIndex, current, aliases, metrics);
        continue;
      }

      String sourceAlias = aliases.get(currentPath.field(relation.sourceColumn()));
      if (sourceAlias == null) {
        long inputRows = current.count();
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
                relation.sourceResource(), relation.targetResource(), step, inputRows));
        currentPath = targetPath;
        rememberPrefix(mapping, relationIndex, current, aliases, metrics);
        continue;
      }
      Dataset<Row> targetRaw = targetRawOpt.get();
      if (!hasColumn(targetRaw, relation.targetColumn())) {
        long inputRows = current.count();
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
                relation.sourceResource(), relation.targetResource(), step, inputRows));
        currentPath = targetPath;
        rememberPrefix(mapping, relationIndex, current, aliases, metrics);
        continue;
      }

      Row parentStats =
          current
              .agg(
                  count(lit(1)).alias("inputRows"),
                  coalesce(sum(when(col(sourceAlias).isNotNull(), 1L).otherwise(0L)), lit(0L))
                      .alias("sourceKeyPresentRows"))
              .first();
      long inputRows = parentStats.getLong(parentStats.fieldIndex("inputRows"));
      long sourceKeyPresentRows =
          parentStats.getLong(parentStats.fieldIndex("sourceKeyPresentRows"));

      Column filterPredicate =
          step.filter().isPresent() ? step.filter().build(FieldColumns.of(targetRaw)) : lit(true);
      Row targetStats =
          targetRaw
              .agg(
                  count(lit(1)).alias("beforeFilter"),
                  coalesce(sum(when(filterPredicate, 1L).otherwise(0L)), lit(0L))
                      .alias("afterFilter"))
              .first();
      long targetRowsBeforeFilter = targetStats.getLong(targetStats.fieldIndex("beforeFilter"));
      long targetRowsAfterFilter = targetStats.getLong(targetStats.fieldIndex("afterFilter"));
      Dataset<Row> filteredTarget =
          step.filter().isPresent() ? targetRaw.filter(filterPredicate) : targetRaw;
      CardinalityStrategy strategy =
          step.cardinalityStrategy().orElseGet(CardinalityStrategy::exactlyOne);
      // Cardinality is about distinct related records, not duplicate physical rows.
      // This is especially important for junction tables where the same relationship may be
      // repeated verbatim; two identical links must not turn EXACTLY_ONE into ambiguity.
      Dataset<Row> cardinalityTarget =
          strategy instanceof CardinalityStrategy.ExactlyOne
              ? filteredTarget.distinct()
              : filteredTarget;

      Map<FieldRef, String> targetAliases = new LinkedHashMap<>();
      Dataset<Row> target = aliasResource(cardinalityTarget, targetPath, targetAliases);
      String targetAlias = targetAliases.get(targetPath.field(relation.targetColumn()));

      Dataset<Row> parent = current.withColumn(INTERNAL_PARENT_ID, monotonically_increasing_id());
      Dataset<Row> joined =
          parent.join(
              target, parent.col(sourceAlias).equalTo(target.col(targetAlias)), "left_outer");

      WindowSpec parentWindow = Window.partitionBy(col(INTERNAL_PARENT_ID));
      joined = joined.withColumn(INTERNAL_MATCH_COUNT, count(col(targetAlias)).over(parentWindow));

      Row joinStats =
          joined
              .select(INTERNAL_PARENT_ID, INTERNAL_MATCH_COUNT)
              .dropDuplicates(INTERNAL_PARENT_ID)
              .agg(
                  coalesce(sum(when(col(INTERNAL_MATCH_COUNT).gt(0), 1L).otherwise(0L)), lit(0L))
                      .alias("matchedParents"),
                  coalesce(sum(when(col(INTERNAL_MATCH_COUNT).gt(1), 1L).otherwise(0L)), lit(0L))
                      .alias("multipleParents"),
                  coalesce(
                          sum(
                              when(col(INTERNAL_MATCH_COUNT).gt(0), col(INTERNAL_MATCH_COUNT))
                                  .otherwise(1L)),
                          lit(0L))
                      .alias("fanOutRows"))
              .first();
      long matchedParentRows = joinStats.getLong(joinStats.fieldIndex("matchedParents"));
      long unmatchedParentRows = inputRows - matchedParentRows;
      long multipleMatchParentRows = joinStats.getLong(joinStats.fieldIndex("multipleParents"));
      long fanOutRows = joinStats.getLong(joinStats.fieldIndex("fanOutRows"));

      joined = applyCardinality(joined, targetAliases, targetPath, strategy, parentWindow);
      joined = joined.drop(INTERNAL_PARENT_ID).drop(INTERNAL_MATCH_COUNT).drop(INTERNAL_ROW_NUMBER);

      long outputRows = strategy instanceof CardinalityStrategy.FanOut ? fanOutRows : inputRows;
      metrics.add(
          new RelationExecutionMetrics(
              relation.sourceResource(),
              relation.targetResource(),
              RelationExecutionMetrics.cardinalityName(step),
              step.requirement().name(),
              step.filter().isPresent(),
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
      rememberPrefix(mapping, relationIndex, current, aliases, metrics);
    }

    metricsCollector.record(mapping.name(), metrics);
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
      // Rewrite all target columns in one projection. Chaining one withColumn per target field
      // creates a deeply nested logical plan for wide DwC-DP resources such as material and can
      // exhaust driver memory during Spark analysis.
      java.util.Set<String> targetAliasNames = new java.util.HashSet<>(targetAliases.values());
      Column[] projected =
          java.util.Arrays.stream(joined.columns())
              .map(
                  name ->
                      targetAliasNames.contains(name)
                          ? when(
                                  col(quote(name))
                                      .isNotNull()
                                      .and(col(INTERNAL_MATCH_COUNT).equalTo(1L)),
                                  col(quote(name)))
                              .otherwise(lit(null))
                              .as(name)
                          : col(quote(name)))
              .toArray(Column[]::new);
      return joined.select(projected).dropDuplicates(INTERNAL_PARENT_ID);
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

  private void rememberPrefix(
      Mapping mapping,
      int relationIndex,
      Dataset<Row> dataset,
      Map<FieldRef, String> aliases,
      List<RelationExecutionMetrics> metrics) {
    prefixCache.remember(
        mapping.sourceResource(),
        mapping.relations().subList(0, relationIndex + 1),
        new SparkPathResult(dataset, new LinkedHashMap<>(aliases)),
        metrics);
  }

  private SchemaPath pathAfter(
      String sourceResource, List<RelationStep> relations, int relationCount) {
    SchemaPath path = SchemaPath.root(sourceResource);
    for (int i = 0; i < relationCount; i++) {
      path =
          path.append(
              SchemaRelationResolver.resolve(graph, path.currentResource(), relations.get(i)));
    }
    return path;
  }

  private Dataset<Row> addNullResource(
      Dataset<Row> dataset, SchemaPath path, String resource, Map<FieldRef, String> aliases) {
    SchemaResource schemaResource =
        graph
            .resource(resource)
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown schema resource: " + resource));
    List<Column> selected =
        java.util.Arrays.stream(dataset.columns())
            .map(name -> col(quote(name)))
            .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
    java.util.Set<String> existing =
        new java.util.HashSet<>(java.util.Arrays.asList(dataset.columns()));
    for (String raw : schemaResource.fields().keySet()) {
      FieldRef ref = path.field(raw);
      String alias = SparkSchemaPathExecutor.physicalAlias(ref);
      aliases.put(ref, alias);
      if (!existing.contains(alias)) {
        selected.add(lit(null).as(alias));
      }
    }
    return dataset.select(selected.toArray(Column[]::new));
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
        .orElseThrow(
            () -> new IllegalArgumentException("Required root resource is absent: " + resource));
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
