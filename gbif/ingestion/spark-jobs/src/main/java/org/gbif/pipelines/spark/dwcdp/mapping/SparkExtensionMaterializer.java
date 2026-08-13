package org.gbif.pipelines.spark.dwcdp.mapping;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.flatten;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sort_array;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Materializes one DwC-A extension mapping without yet attaching it to an Event/Occurrence core.
 *
 * <p>A fragment is always grouped by the primary key of its source resource. A fragment declaring
 * {@code rowIdentity(...)} defines extension rows beneath that source key. Fragments without a row
 * identity are source-scope enrichments: they are aggregated to one row per source key and merged
 * onto every extension row defined by the row-producing fragment. This deliberately avoids joining
 * two fan-out paths together before aggregation.
 */
public final class SparkExtensionMaterializer {
  static final String COL_PARENT_KEY = "__dwca_parent_key";
  static final String COL_ROW_KEY = "__dwca_row_key";

  private final SchemaGraph graph;
  private final SparkMappingPathExecutor pathExecutor;

  public SparkExtensionMaterializer(SchemaGraph graph) {
    this.graph = graph;
    this.pathExecutor = new SparkMappingPathExecutor(graph);
  }

  public ExtensionMaterializationResult materialize(
      TableLoader loader, ExtensionMapping extension) {
    if (extension.fragments().isEmpty()) {
      throw new IllegalArgumentException("Extension has no fragments: " + extension.rowType());
    }

    List<ExtensionFragment> rowFragments =
        extension.fragments().stream().filter(f -> f.rowIdentityColumn().isPresent()).toList();
    if (rowFragments.size() > 1) {
      throw new IllegalArgumentException(
          "Extension " + extension.rowType()
              + " has multiple row-defining fragments; explicit row-set composition is not supported yet: "
              + rowFragments.stream().map(ExtensionFragment::name).toList());
    }

    ExtensionFragment base = rowFragments.isEmpty() ? extension.fragments().get(0) : rowFragments.get(0);
    for (ExtensionFragment fragment : extension.fragments()) {
      if (!base.sourceResource().equals(fragment.sourceResource())) {
        throw new IllegalArgumentException(
            "Fragments currently need the same source resource so parent keys share one scope: "
                + base.name() + " starts at " + base.sourceResource()
                + ", but " + fragment.name() + " starts at " + fragment.sourceResource());
      }
    }
    FragmentResult materializedBase = materializeFragment(loader, base, true);
    Dataset<Row> current = materializedBase.dataset();
    Map<String, String> targetColumns = new LinkedHashMap<>(materializedBase.targetColumns());

    for (ExtensionFragment fragment : extension.fragments()) {
      if (fragment == base) {
        continue;
      }
      if (fragment.rowIdentityColumn().isPresent()) {
        throw new IllegalArgumentException(
            "Only one row-defining fragment is supported per extension for now: " + fragment.name());
      }

      FragmentResult enrichment = materializeFragment(loader, fragment, false);
      ensureNoDuplicateTargets(targetColumns, enrichment.targetColumns(), fragment.name());

      Dataset<Row> enrichmentForJoin = enrichment.dataset().drop(COL_ROW_KEY);
      current =
          current.join(
                  enrichmentForJoin,
                  current.col(COL_PARENT_KEY).equalTo(enrichmentForJoin.col(COL_PARENT_KEY)),
                  "left_outer")
              .drop(enrichmentForJoin.col(COL_PARENT_KEY));
      targetColumns.putAll(enrichment.targetColumns());
    }

    return new ExtensionMaterializationResult(
        current, COL_PARENT_KEY, COL_ROW_KEY, targetColumns);
  }

  private FragmentResult materializeFragment(
      TableLoader loader, ExtensionFragment fragment, boolean rowProducing) {
    SchemaResource source =
        graph.resource(fragment.sourceResource())
            .orElseThrow(() -> new IllegalArgumentException(
                "Unknown fragment source resource: " + fragment.sourceResource()));
    String sourcePk =
        source.primaryKey()
            .orElseThrow(() -> new IllegalArgumentException(
                "Fragment source has no primary key: " + fragment.sourceResource()));

    Mapping mapping =
        new Mapping(
            "extension-fragment:" + fragment.name(),
            fragment.sourceResource(),
            fragment.relations(),
            List.of(),
            Projection.none());
    MappingExecutionResult execution = pathExecutor.execute(loader, mapping);
    if (!execution.completePath()) {
      // Optional path missing: contribute no rows. Keep a zero-row shape based on the source table.
      Dataset<Row> empty = execution.pathResult().dataset().limit(0)
          .select(lit(null).cast("string").as(COL_PARENT_KEY),
              lit(null).cast("string").as(COL_ROW_KEY));
      return new FragmentResult(empty, Map.of());
    }

    SparkPathResult pathResult = execution.pathResult();
    SchemaPath sourcePath = SchemaPath.root(fragment.sourceResource());
    String parentAlias = pathResult.columnName(sourcePath.field(sourcePk));

    SchemaPath finalPath = resolveFinalPath(fragment);
    Optional<String> identity = fragment.rowIdentityColumn();
    String identityAlias =
        identity.map(column -> pathResult.columnName(finalPath.field(column))).orElse(parentAlias);

    // Build stable internal names first, then aggregate. Target term strings may be URIs.
    Map<String, String> targetAliases = new LinkedHashMap<>();
    List<Column> aggregates = new ArrayList<>();
    for (TargetFieldMapping field : fragment.fields()) {
      String alias = targetAlias(field.targetTerm());
      targetAliases.put(field.targetTerm(), alias);
      aggregates.add(aggregateExpression(field, pathResult).as(alias));
    }

    boolean distinctRowIdentity = rowProducing && identity.isPresent() && !identityAlias.equals(parentAlias);
    Dataset<Row> grouped;
    if (aggregates.isEmpty()) {
      if (distinctRowIdentity) {
        grouped =
            pathResult.dataset()
                .select(
                    col(parentAlias).cast("string").as(COL_PARENT_KEY),
                    col(identityAlias).cast("string").as(COL_ROW_KEY))
                .distinct();
      } else {
        grouped =
            pathResult.dataset()
                .select(col(parentAlias).cast("string").as(COL_PARENT_KEY))
                .distinct()
                .withColumn(COL_ROW_KEY, col(COL_PARENT_KEY));
      }
    } else {
      Column[] aggArray = aggregates.toArray(Column[]::new);
      if (distinctRowIdentity) {
        grouped =
            pathResult.dataset()
                .groupBy(col(parentAlias), col(identityAlias))
                .agg(aggArray[0], java.util.Arrays.copyOfRange(aggArray, 1, aggArray.length))
                .withColumnRenamed(parentAlias, COL_PARENT_KEY)
                .withColumnRenamed(identityAlias, COL_ROW_KEY);
      } else {
        grouped =
            pathResult.dataset()
                .groupBy(col(parentAlias))
                .agg(aggArray[0], java.util.Arrays.copyOfRange(aggArray, 1, aggArray.length))
                .withColumnRenamed(parentAlias, COL_PARENT_KEY)
                .withColumn(COL_ROW_KEY, col(COL_PARENT_KEY));
      }
    }

    // If parent and row identity are the same physical field, Spark only keeps one group column.
    if (!hasColumn(grouped, COL_ROW_KEY)) {
      grouped = grouped.withColumn(COL_ROW_KEY, col(COL_PARENT_KEY).cast("string"));
    }
    grouped = grouped.withColumn(COL_PARENT_KEY, col(COL_PARENT_KEY).cast("string"));
    return new FragmentResult(grouped, targetAliases);
  }

  private Column aggregateExpression(TargetFieldMapping field, SparkPathResult pathResult) {
    List<Column> sources = field.sources().stream().map(pathResult::column).toList();

    if (field.sourceMode() == TargetFieldMapping.SourceMode.ONE_OF
        && field.aggregation() instanceof ValueAggregation.FirstNonNull) {
      return first(coalesce(sources.toArray(Column[]::new)), true);
    }

    if (field.aggregation() instanceof ValueAggregation.Delimited delimited) {
      Column values = flatten(collect_list(array(sources.toArray(Column[]::new))));
      if (delimited.distinct()) {
        values = array_distinct(values);
      }
      return concat_ws(delimited.delimiter(), sort_array(values));
    }

    throw new UnsupportedOperationException(
        "Unsupported target aggregation for " + field.targetTerm() + ": " + field.aggregation());
  }

  private SchemaPath resolveFinalPath(ExtensionFragment fragment) {
    SchemaPath path = SchemaPath.root(fragment.sourceResource());
    for (RelationStep step : fragment.relations()) {
      SchemaRelation relation =
          graph.resolve(
              path.currentResource(),
              step.targetResource(),
              step.viaColumn().orElse(null),
              step.schemaPredicate().orElse(null));
      path = path.append(relation);
    }
    return path;
  }

  private static void ensureNoDuplicateTargets(
      Map<String, String> existing, Map<String, String> incoming, String fragment) {
    for (String target : incoming.keySet()) {
      if (existing.containsKey(target)) {
        throw new IllegalArgumentException(
            "Target term " + target + " is produced by multiple fragments; explicit target merge semantics "
                + "are required before importing fragment " + fragment);
      }
    }
  }

  private static String targetAlias(String term) {
    return "__dwca_term__" + shortHash(term);
  }

  private static String shortHash(String value) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder out = new StringBuilder();
      for (int i = 0; i < 8; i++) {
        out.append(String.format("%02x", digest[i]));
      }
      return out.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  private static boolean hasColumn(Dataset<Row> dataset, String name) {
    for (String column : dataset.columns()) {
      if (column.equals(name)) {
        return true;
      }
    }
    return false;
  }

  private record FragmentResult(Dataset<Row> dataset, Map<String, String> targetColumns) {}
}
