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
import static org.apache.spark.sql.functions.monotonically_increasing_id;
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
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledTargetProducer;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingCompiler;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Materializes one DwC-A extension mapping without yet attaching it to an Event/Occurrence core.
 *
 * <p>Logical source identity is retained as {@link FieldRef} all the way through target compilation.
 * Spark aliases are only physical bindings of those logical fields. Duplicate target validation is
 * therefore able to report both producers, their complete path-qualified sources, and their Spark
 * aliases instead of only reporting the target URI.
 */
public final class SparkExtensionMaterializer {
  static final String COL_PARENT_KEY = "__dwca_parent_key";
  static final String COL_ROW_KEY = "__dwca_row_key";

  private final SchemaGraph graph;
  private final SparkMappingPathExecutor pathExecutor;
  private final MappingCompiler compiler;

  public SparkExtensionMaterializer(SchemaGraph graph) {
    this.graph = graph;
    this.pathExecutor = new SparkMappingPathExecutor(graph);
    this.compiler = new MappingCompiler(graph);
  }

  /** Convenience boundary for callers that still hold declarative configuration. */
  public ExtensionMaterializationResult materialize(
      TableLoader loader, ExtensionMapping extension) {
    return materialize(loader, compiler.compile(extension));
  }

  /** Materializes an already schema-resolved extension. */
  public ExtensionMaterializationResult materialize(
      TableLoader loader, CompiledExtension extension) {
    if (extension.fragments().isEmpty()) {
      throw new IllegalArgumentException("Extension has no fragments: " + extension.rowType());
    }

    List<CompiledFragment> rowFragments =
        extension.fragments().stream().filter(f -> f.rowIdentity().isPresent()).toList();
    if (rowFragments.size() > 1) {
      throw new IllegalArgumentException(
          "Extension " + extension.rowType()
              + " has multiple row-defining fragments; explicit row-set composition is not supported yet: "
              + rowFragments.stream().map(CompiledFragment::name).toList());
    }

    CompiledFragment base =
        rowFragments.isEmpty() ? extension.fragments().get(0) : rowFragments.get(0);
    for (CompiledFragment fragment : extension.fragments()) {
      if (!base.sourceResource().equals(fragment.sourceResource())) {
        throw new IllegalArgumentException(
            "Fragments currently need the same source resource so parent keys share one scope: "
                + base.name() + " starts at " + base.sourceResource()
                + ", but " + fragment.name() + " starts at " + fragment.sourceResource());
      }
      if (!base.scopeKey().equals(fragment.scopeKey())) {
        throw new IllegalArgumentException(
            "Fragments currently need the same scope key: "
                + base.name() + " uses " + base.scopeKey().qualifiedName()
                + ", but " + fragment.name() + " uses " + fragment.scopeKey().qualifiedName());
      }
    }

    FragmentResult materializedBase = materializeFragment(loader, base, true);
    Dataset<Row> current = materializedBase.dataset();
    Map<String, MaterializedTarget> targets =
        new LinkedHashMap<>(materializedBase.targets());

    for (CompiledFragment fragment : extension.fragments()) {
      if (fragment == base) {
        continue;
      }
      if (fragment.rowIdentity().isPresent()) {
        throw new IllegalArgumentException(
            "Only one row-defining fragment is supported per extension for now: " + fragment.name());
      }

      FragmentResult enrichment = materializeFragment(loader, fragment, false);
      ensureNoDuplicateTargets(targets, enrichment.targets());

      Dataset<Row> enrichmentForJoin = enrichment.dataset().drop(COL_ROW_KEY);
      current =
          current.join(
                  enrichmentForJoin,
                  current.col(COL_PARENT_KEY).equalTo(enrichmentForJoin.col(COL_PARENT_KEY)),
                  "left_outer")
              .drop(enrichmentForJoin.col(COL_PARENT_KEY));
      targets.putAll(enrichment.targets());
    }

    Map<String, String> targetColumns = new LinkedHashMap<>();
    targets.forEach((term, target) -> targetColumns.put(term, target.physicalColumn()));
    return new ExtensionMaterializationResult(
        current, COL_PARENT_KEY, materializedBase.parentKeySource(), COL_ROW_KEY, targetColumns);
  }

  private FragmentResult materializeFragment(
      TableLoader loader, CompiledFragment fragment, boolean rowProducing) {
    graph.resource(fragment.sourceResource())
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Unknown fragment source resource: " + fragment.sourceResource()));

    Mapping mapping =
        new Mapping(
            "extension-fragment:" + fragment.name(),
            fragment.sourceResource(),
            fragment.relations().stream().map(r -> r.toRelationStep()).toList(),
            List.of(),
            Projection.none());
    MappingExecutionResult execution = pathExecutor.execute(loader, mapping);
    if (!execution.completePath()) {
      Dataset<Row> empty =
          execution.pathResult().dataset().limit(0)
              .select(
                  lit(null).cast("string").as(COL_PARENT_KEY),
                  lit(null).cast("string").as(COL_ROW_KEY));
      return new FragmentResult(empty, fragment.scopeKey(), Map.of());
    }

    SparkPathResult pathResult = execution.pathResult();
    FieldRef parentKeySource = fragment.scopeKey();
    String parentAlias = pathResult.columnName(parentKeySource);

    Optional<FieldRef> identity = fragment.rowIdentity();
    String identityAlias = identity.map(pathResult::columnName).orElse(parentAlias);

    Map<String, MaterializedTarget> targets = new LinkedHashMap<>();
    List<Column> aggregates = new ArrayList<>();
    for (CompiledTargetProducer target : fragment.targets()) {
      String alias = targetAlias(target.targetTerm());
      MaterializedTarget materializedTarget = bindTarget(target, alias, pathResult);
      MaterializedTarget previous = targets.putIfAbsent(target.targetTerm(), materializedTarget);
      if (previous != null) {
        throw duplicateTargetException(target.targetTerm(), previous, materializedTarget);
      }
      if (!(rowProducing && fragment.rowIdentity().isEmpty())) {
        aggregates.add(aggregateExpression(target, pathResult).as(alias));
      }
    }

    boolean distinctRowIdentity =
        rowProducing && identity.isPresent() && !identityAlias.equals(parentAlias);
    Dataset<Row> grouped;
    if (rowProducing && identity.isEmpty()) {
      // No declared logical key means each physical source/result row is an extension row. This is
      // important for legitimate keyless child tables such as event-identifier: grouping only by
      // the parent scope would incorrectly collapse all identifiers into one row. The synthetic
      // row key is execution-internal only; it is never emitted as a DwC-A term.
      List<Column> selected = new ArrayList<>();
      selected.add(col(parentAlias).cast("string").as(COL_PARENT_KEY));
      selected.add(monotonically_increasing_id().cast("string").as(COL_ROW_KEY));
      for (CompiledTargetProducer target : fragment.targets()) {
        selected.add(rowExpression(target, pathResult).as(targetAlias(target.targetTerm())));
      }
      grouped = pathResult.dataset().select(selected.toArray(Column[]::new));
    } else if (aggregates.isEmpty()) {
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

    if (!hasColumn(grouped, COL_ROW_KEY)) {
      grouped = grouped.withColumn(COL_ROW_KEY, col(COL_PARENT_KEY).cast("string"));
    }
    grouped = grouped.withColumn(COL_PARENT_KEY, col(COL_PARENT_KEY).cast("string"));
    return new FragmentResult(grouped, parentKeySource, targets);
  }

  private static MaterializedTarget bindTarget(
      CompiledTargetProducer target, String targetAlias, SparkPathResult pathResult) {
    List<MaterializedSourceField> sources =
        target.sources().stream()
            .map(
                source ->
                    new MaterializedSourceField(
                        source, Optional.ofNullable(pathResult.aliases().get(source.field()))))
            .toList();
    return new MaterializedTarget(target, targetAlias, sources);
  }

  private Column rowExpression(
      CompiledTargetProducer target, SparkPathResult pathResult) {
    List<Column> sources =
        target.sources().stream().map(source -> pathResult.columnOrNull(source.field())).toList();

    if (target.sourceMode() == TargetFieldMapping.SourceMode.ONE_OF
        && target.aggregation() instanceof ValueAggregation.FirstNonNull) {
      return coalesce(sources.toArray(Column[]::new));
    }
    if (target.aggregation() instanceof ValueAggregation.ExactlyOne && sources.size() == 1) {
      return sources.get(0);
    }

    throw new UnsupportedOperationException(
        "Unsupported row-level target aggregation for "
            + target.targetTerm()
            + ": "
            + target.aggregation());
  }

  private Column aggregateExpression(
      CompiledTargetProducer target, SparkPathResult pathResult) {
    List<Column> sources =
        target.sources().stream().map(source -> pathResult.columnOrNull(source.field())).toList();

    if (target.sourceMode() == TargetFieldMapping.SourceMode.ONE_OF
        && target.aggregation() instanceof ValueAggregation.FirstNonNull) {
      return first(coalesce(sources.toArray(Column[]::new)), true);
    }

    if (target.aggregation() instanceof ValueAggregation.Delimited delimited) {
      Column values = flatten(collect_list(array(sources.toArray(Column[]::new))));
      if (delimited.distinct()) {
        values = array_distinct(values);
      }
      return concat_ws(delimited.delimiter(), sort_array(values));
    }

    throw new UnsupportedOperationException(
        "Unsupported target aggregation for "
            + target.targetTerm()
            + ": "
            + target.aggregation());
  }

  private static void ensureNoDuplicateTargets(
      Map<String, MaterializedTarget> existing, Map<String, MaterializedTarget> incoming) {
    for (Map.Entry<String, MaterializedTarget> entry : incoming.entrySet()) {
      MaterializedTarget previous = existing.get(entry.getKey());
      if (previous != null) {
        throw duplicateTargetException(entry.getKey(), previous, entry.getValue());
      }
    }
  }

  private static IllegalStateException duplicateTargetException(
      String target, MaterializedTarget existing, MaterializedTarget incoming) {
    return new IllegalStateException(
        "Compiler invariant violated: duplicate target producer reached Spark materialization\n\n"
            + "Target:\n  " + target + "\n\n"
            + "Existing producer:\n" + indent(existing.describe(), "  ") + "\n\n"
            + "Incoming producer:\n" + indent(incoming.describe(), "  ") + "\n\n"
            + "Target ownership must be resolved by MappingCompiler before Spark execution.");
  }

  private static String indent(String text, String indent) {
    return indent + text.replace("\n", "\n" + indent);
  }

  private static String targetAlias(String term) {
    return "__dwca_term__" + shortHash(term);
  }

  private static String shortHash(String value) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
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

  private record FragmentResult(
      Dataset<Row> dataset, FieldRef parentKeySource, Map<String, MaterializedTarget> targets) {}
}
