package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.sort_array;
import static org.apache.spark.sql.functions.struct;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledTargetMerge;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledTargetProducer;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionRowComposition;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.Mapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.Projection;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Materializes one DwC-A extension mapping without yet attaching it to an Event/Occurrence core.
 *
 * <p>Logical source identity is retained as {@link FieldRef} all the way through target
 * compilation. Spark aliases are only physical bindings of those logical fields. Duplicate target
 * validation is therefore able to report both producers, their complete path-qualified sources, and
 * their Spark aliases instead of only reporting the target URI.
 */
public final class SparkExtensionMaterializer {
  static final String COL_PARENT_KEY = "__dwca_parent_key";
  static final String COL_ROW_KEY = "__dwca_row_key";

  private final SchemaGraph graph;
  private final SparkMappingPathExecutor pathExecutor;
  private final MappingCompiler compiler;

  public SparkExtensionMaterializer(SchemaGraph graph) {
    this(graph, new ExecutionMetricsCollector());
  }

  SparkExtensionMaterializer(SchemaGraph graph, ExecutionMetricsCollector metricsCollector) {
    this(graph, metricsCollector, SparkPathPrefixCache.disabled());
  }

  SparkExtensionMaterializer(
      SchemaGraph graph,
      ExecutionMetricsCollector metricsCollector,
      SparkPathPrefixCache prefixCache) {
    this.graph = graph;
    this.pathExecutor = new SparkMappingPathExecutor(graph, metricsCollector, prefixCache);
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
    return extension.rowComposition() == ExtensionRowComposition.UNION
        ? materializeUnion(loader, extension)
        : materializeEnriched(loader, extension);
  }

  private ExtensionMaterializationResult materializeEnriched(
      TableLoader loader, CompiledExtension extension) {
    List<CompiledFragment> rowFragments =
        extension.fragments().stream().filter(f -> f.rowIdentity().isPresent()).toList();
    if (rowFragments.size() > 1) {
      throw new IllegalArgumentException(
          "Extension "
              + extension.rowType()
              + " has multiple row-defining fragments; declare UNION row composition when fragments are independent row sets: "
              + rowFragments.stream().map(CompiledFragment::name).toList());
    }

    CompiledFragment base =
        rowFragments.isEmpty() ? extension.fragments().get(0) : rowFragments.get(0);
    for (CompiledFragment fragment : extension.fragments()) {
      if (!base.sourceResource().equals(fragment.sourceResource())) {
        throw new IllegalArgumentException(
            "ENRICH fragments need the same source resource: "
                + base.name()
                + " starts at "
                + base.sourceResource()
                + ", but "
                + fragment.name()
                + " starts at "
                + fragment.sourceResource());
      }
      if (!base.scopeKey().equals(fragment.scopeKey())) {
        throw new IllegalArgumentException(
            "ENRICH fragments need the same scope key: "
                + base.name()
                + " uses "
                + base.scopeKey().qualifiedName()
                + ", but "
                + fragment.name()
                + " uses "
                + fragment.scopeKey().qualifiedName());
      }
    }

    Set<String> mergeTerms =
        extension.targetMerges().stream()
            .map(CompiledTargetMerge::targetTerm)
            .collect(java.util.stream.Collectors.toCollection(HashSet::new));
    FragmentResult materializedBase = materializeFragment(loader, base, true, false, mergeTerms);
    Dataset<Row> current = materializedBase.dataset();
    Map<String, MaterializedTarget> targets = new LinkedHashMap<>();
    Map<String, List<MaterializedTarget>> mergeContributions = new LinkedHashMap<>();
    collectTargets(materializedBase.targets(), mergeTerms, targets, mergeContributions);

    for (CompiledFragment fragment : extension.fragments()) {
      if (fragment == base) {
        continue;
      }
      if (fragment.rowIdentity().isPresent()) {
        throw new IllegalArgumentException(
            "Only one row-defining fragment is supported for ENRICH composition: "
                + fragment.name());
      }

      FragmentResult enrichment = materializeFragment(loader, fragment, false, false, mergeTerms);
      ensureNoDuplicateTargets(targets, enrichment.targets(), mergeTerms);

      Dataset<Row> enrichmentForJoin = enrichment.dataset();
      Column joinCondition =
          current.col(COL_PARENT_KEY).equalTo(enrichmentForJoin.col(COL_PARENT_KEY));
      if (fragment.rowMatch().isPresent()) {
        joinCondition =
            joinCondition.and(current.col(COL_ROW_KEY).equalTo(enrichmentForJoin.col(COL_ROW_KEY)));
      }
      current =
          current
              .join(enrichmentForJoin, joinCondition, "left_outer")
              .drop(enrichmentForJoin.col(COL_PARENT_KEY))
              .drop(enrichmentForJoin.col(COL_ROW_KEY));
      collectTargets(enrichment.targets(), mergeTerms, targets, mergeContributions);
    }

    Map<String, String> targetColumns = new LinkedHashMap<>();
    targets.forEach((term, target) -> targetColumns.put(term, target.physicalColumn()));
    for (CompiledTargetMerge merge : extension.targetMerges()) {
      Dataset<Row> merged = materializeExtensionTargetMerge(loader, extension, merge);
      String alias = targetAlias(merge.targetTerm());
      Column joinCondition =
          current
              .col(COL_PARENT_KEY)
              .equalTo(merged.col(COL_PARENT_KEY))
              .and(current.col(COL_ROW_KEY).equalTo(merged.col(COL_ROW_KEY)));
      current =
          current
              .join(merged, joinCondition, "left_outer")
              .drop(merged.col(COL_PARENT_KEY))
              .drop(merged.col(COL_ROW_KEY));
      targetColumns.put(merge.targetTerm(), alias);
    }
    current = filterEmptyPayloadRows(current, targetColumns.values().stream().toList());
    current =
        applyRowLimit(
            current, targetColumns.values().stream().toList(), extension.maxRowsPerParent());
    return new ExtensionMaterializationResult(
        current, COL_PARENT_KEY, materializedBase.parentKeySource(), COL_ROW_KEY, targetColumns);
  }

  private ExtensionMaterializationResult materializeUnion(
      TableLoader loader, CompiledExtension extension) {
    List<FragmentResult> rows = new ArrayList<>();
    List<CompiledFragment> enrichments = new ArrayList<>();
    Map<String, MaterializedTarget> targets = new LinkedHashMap<>();

    for (CompiledFragment fragment : extension.fragments()) {
      if (fragment.rowMatch().isPresent()) {
        enrichments.add(fragment);
        continue;
      }
      if (loader.load(fragment.sourceResource()).isEmpty()) {
        continue;
      }
      FragmentResult materialized = materializeFragment(loader, fragment, true, true, Set.of());
      if (fragment.rowIdentity().isPresent()) {
        materialized =
            new FragmentResult(
                materialized.dataset().filter(col(COL_ROW_KEY).isNotNull()),
                materialized.parentKeySource(),
                materialized.targets());
      }
      rows.add(materialized);
      materialized.targets().forEach(targets::putIfAbsent);
    }

    if (rows.isEmpty()) {
      CompiledFragment first = extension.fragments().get(0);
      Dataset<Row> empty =
          loader
              .load(first.sourceResource())
              .map(
                  df ->
                      df.limit(0)
                          .select(
                              lit(null).cast("string").as(COL_PARENT_KEY),
                              lit(null).cast("string").as(COL_ROW_KEY)))
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "No UNION fragment source is present for extension "
                              + extension.rowType()));
      return new ExtensionMaterializationResult(
          empty, COL_PARENT_KEY, first.scopeKey(), COL_ROW_KEY, Map.of());
    }

    Dataset<Row> combined = rows.get(0).dataset();
    for (int i = 1; i < rows.size(); i++) {
      combined = combined.unionByName(rows.get(i).dataset(), true);
    }

    for (CompiledFragment fragment : enrichments) {
      if (loader.load(fragment.sourceResource()).isEmpty()) {
        continue;
      }
      FragmentResult enrichment = materializeFragment(loader, fragment, false, false, Set.of());
      ensureNoDuplicateTargets(targets, enrichment.targets(), Set.of());

      Dataset<Row> enrichmentForJoin = enrichment.dataset();
      Column joinCondition = combined.col(COL_ROW_KEY).equalTo(enrichmentForJoin.col(COL_ROW_KEY));
      // When an enrichment is scoped by the same logical field it matches, row identity alone is
      // sufficient. Otherwise keep the enrichment parent-scoped to avoid cross-parent matches.
      if (!fragment.scopeKey().equals(fragment.rowMatch().orElseThrow())) {
        joinCondition =
            combined
                .col(COL_PARENT_KEY)
                .equalTo(enrichmentForJoin.col(COL_PARENT_KEY))
                .and(joinCondition);
      }
      combined =
          combined
              .join(enrichmentForJoin, joinCondition, "left_outer")
              .drop(enrichmentForJoin.col(COL_PARENT_KEY))
              .drop(enrichmentForJoin.col(COL_ROW_KEY));
      enrichment.targets().forEach(targets::putIfAbsent);
    }

    // UNION row identity is the visible extension payload within one parent, matching the legacy
    // media path's dropDuplicates() behaviour. Synthetic execution row keys must not make otherwise
    // identical rows look distinct.
    List<String> dedupeColumns = new ArrayList<>();
    dedupeColumns.add(COL_PARENT_KEY);
    targets.values().stream()
        .map(MaterializedTarget::physicalColumn)
        .sorted()
        .forEach(dedupeColumns::add);
    combined = combined.dropDuplicates(dedupeColumns.toArray(String[]::new));

    Map<String, String> targetColumns = new LinkedHashMap<>();
    targets.forEach((term, target) -> targetColumns.put(term, target.physicalColumn()));
    combined =
        applyRowLimit(
            combined, targetColumns.values().stream().toList(), extension.maxRowsPerParent());
    return new ExtensionMaterializationResult(
        combined, COL_PARENT_KEY, rows.get(0).parentKeySource(), COL_ROW_KEY, targetColumns);
  }

  private static Dataset<Row> filterEmptyPayloadRows(
      Dataset<Row> rows, List<String> targetColumns) {
    if (targetColumns.isEmpty()) {
      return rows.limit(0);
    }
    Column hasPayload = null;
    for (String targetColumn : targetColumns) {
      Column present = col(targetColumn).isNotNull();
      hasPayload = hasPayload == null ? present : hasPayload.or(present);
    }
    return rows.filter(hasPayload);
  }

  /**
   * Applies a deterministic per-parent cap using only visible target payload. Execution-only row
   * identity is deliberately excluded from ordering so the policy is stable across retries and does
   * not leak physical Spark identity into mapping semantics.
   */
  private static Dataset<Row> applyRowLimit(
      Dataset<Row> rows, List<String> targetColumns, Optional<Integer> maxRowsPerParent) {
    if (maxRowsPerParent.isEmpty()) {
      return rows;
    }

    List<String> stableColumns = targetColumns.stream().sorted().toList();
    Column stableRow =
        stableColumns.isEmpty()
            ? lit("")
            : org.apache.spark.sql.functions.to_json(
                org.apache.spark.sql.functions.struct(
                    stableColumns.stream()
                        .map(org.apache.spark.sql.functions::col)
                        .toArray(Column[]::new)));
    Column stableHash = org.apache.spark.sql.functions.sha2(stableRow, 256);
    String rankColumn = "__dwca_parent_row_rank";

    return rows.withColumn(
            rankColumn,
            org.apache
                .spark
                .sql
                .functions
                .row_number()
                .over(Window.partitionBy(COL_PARENT_KEY).orderBy(stableHash, stableRow)))
        .filter(col(rankColumn).leq(maxRowsPerParent.get()))
        .drop(rankColumn);
  }

  private FragmentResult materializeFragment(
      TableLoader loader,
      CompiledFragment fragment,
      boolean rowProducing,
      boolean filterEmptyPayload,
      Set<String> mergeTerms) {
    graph
        .resource(fragment.sourceResource())
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
          execution
              .pathResult()
              .dataset()
              .limit(0)
              .select(
                  lit(null).cast("string").as(COL_PARENT_KEY),
                  lit(null).cast("string").as(COL_ROW_KEY));
      return new FragmentResult(empty, fragment.scopeKey(), Map.of());
    }

    SparkPathResult pathResult = execution.pathResult();
    FieldRef parentKeySource = fragment.scopeKey();
    String parentAlias = pathResult.columnName(parentKeySource);

    Optional<FieldRef> identity = rowProducing ? fragment.rowIdentity() : fragment.rowMatch();
    String identityAlias = identity.map(pathResult::columnName).orElse(parentAlias);

    Map<String, MaterializedTarget> targets = new LinkedHashMap<>();
    List<Column> aggregates = new ArrayList<>();
    for (CompiledTargetProducer target : fragment.targets()) {
      String alias = targetAlias(target.targetTerm(), target.owner(), mergeTerms);
      MaterializedTarget materializedTarget = bindTarget(target, alias, pathResult);
      MaterializedTarget previous = targets.putIfAbsent(target.targetTerm(), materializedTarget);
      if (previous != null) {
        throw duplicateTargetException(target.targetTerm(), previous, materializedTarget);
      }
      if (!mergeTerms.contains(target.targetTerm())
          && !(rowProducing && fragment.rowIdentity().isEmpty())) {
        aggregates.add(aggregateExpression(target, pathResult).as(alias));
      }
    }

    boolean distinctRowIdentity = identity.isPresent() && !identityAlias.equals(parentAlias);
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
        if (!mergeTerms.contains(target.targetTerm())) {
          selected.add(
              rowExpression(target, pathResult)
                  .as(targetAlias(target.targetTerm(), target.owner(), mergeTerms)));
        }
      }
      grouped = pathResult.dataset().select(selected.toArray(Column[]::new));
    } else if (aggregates.isEmpty()) {
      if (distinctRowIdentity) {
        grouped =
            pathResult
                .dataset()
                .select(
                    col(parentAlias).cast("string").as(COL_PARENT_KEY),
                    col(identityAlias).cast("string").as(COL_ROW_KEY))
                .distinct();
      } else {
        grouped =
            pathResult
                .dataset()
                .select(col(parentAlias).cast("string").as(COL_PARENT_KEY))
                .distinct()
                .withColumn(COL_ROW_KEY, col(COL_PARENT_KEY));
      }
    } else {
      Column[] aggArray = aggregates.toArray(Column[]::new);
      if (distinctRowIdentity) {
        grouped =
            pathResult
                .dataset()
                .groupBy(col(parentAlias), col(identityAlias))
                .agg(aggArray[0], java.util.Arrays.copyOfRange(aggArray, 1, aggArray.length))
                .withColumnRenamed(parentAlias, COL_PARENT_KEY)
                .withColumnRenamed(identityAlias, COL_ROW_KEY);
      } else {
        grouped =
            pathResult
                .dataset()
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

    // UNION branches are independent row sets, so a branch rooted above optional links must not
    // contribute a synthetic empty row when none of its payload-producing paths resolve. ENRICH
    // filtering happens only after every enrichment fragment has been joined, otherwise a base row
    // can be removed before another fragment supplies its payload (e.g. Humboldt protocol fields).
    if (rowProducing && filterEmptyPayload) {
      grouped =
          filterEmptyPayloadRows(
              grouped, targets.values().stream().map(MaterializedTarget::physicalColumn).toList());
    }
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

  private Column rowExpression(CompiledTargetProducer target, SparkPathResult pathResult) {
    List<Column> sources =
        target.sources().stream().map(source -> pathResult.columnOrNull(source.field())).toList();
    return SparkTargetExpression.row(target, sources);
  }

  private Column aggregateExpression(CompiledTargetProducer target, SparkPathResult pathResult) {
    List<Column> sources =
        target.sources().stream().map(source -> pathResult.columnOrNull(source.field())).toList();
    Optional<Column> contributionIdentity =
        target.contributionIdentity().map(source -> pathResult.columnOrNull(source.field()));
    Optional<Column> orderBy =
        target.orderBy().map(source -> pathResult.columnOrNull(source.field()));
    return SparkTargetExpression.aggregate(target, sources, contributionIdentity, orderBy);
  }

  private static void ensureNoDuplicateTargets(
      Map<String, MaterializedTarget> existing,
      Map<String, MaterializedTarget> incoming,
      Set<String> mergeTerms) {
    for (Map.Entry<String, MaterializedTarget> entry : incoming.entrySet()) {
      if (mergeTerms.contains(entry.getKey())) {
        continue;
      }
      MaterializedTarget previous = existing.get(entry.getKey());
      if (previous != null) {
        throw duplicateTargetException(entry.getKey(), previous, entry.getValue());
      }
    }
  }

  private static void collectTargets(
      Map<String, MaterializedTarget> incoming,
      Set<String> mergeTerms,
      Map<String, MaterializedTarget> ordinary,
      Map<String, List<MaterializedTarget>> merged) {
    incoming.forEach(
        (term, target) -> {
          if (mergeTerms.contains(term)) {
            merged.computeIfAbsent(term, ignored -> new ArrayList<>()).add(target);
          } else {
            ordinary.put(term, target);
          }
        });
  }

  /**
   * Materializes an extension target merge from raw contribution rows rather than from each
   * fragment's already-aggregated value. This is the extension analogue of core target merging and
   * preserves contribution identity and ordering across independent relation paths.
   */
  private Dataset<Row> materializeExtensionFirstNonNullMerge(
      TableLoader loader, CompiledExtension extension, CompiledTargetMerge merge) {
    Dataset<Row> contributions = null;
    int producerOrder = 0;

    for (CompiledTargetProducer producer : merge.producers()) {
      CompiledFragment fragment =
          extension.fragments().stream()
              .filter(candidate -> candidate.name().equals(producer.owner()))
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Merged extension producer references unknown fragment: "
                              + producer.owner()));
      Mapping mapping =
          new Mapping(
              "extension-first-non-null-merge:" + fragment.name(),
              fragment.sourceResource(),
              fragment.relations().stream().map(r -> r.toRelationStep()).toList(),
              List.of(),
              Projection.none());
      SparkPathResult pathResult = pathExecutor.execute(loader, mapping).pathResult();
      FieldRef parentKey = fragment.scopeKey();
      FieldRef rowKey =
          fragment.rowIdentity().orElseGet(() -> fragment.rowMatch().orElse(parentKey));

      String parentAlias = pathResult.columnName(parentKey);
      String rowAlias = pathResult.columnName(rowKey);
      Dataset<Row> contribution =
          pathResult
              .dataset()
              .groupBy(
                  pathResult.dataset().col(parentAlias).cast("string").as(COL_PARENT_KEY),
                  pathResult.dataset().col(rowAlias).cast("string").as(COL_ROW_KEY))
              .agg(
                  aggregateExpression(producer, pathResult).cast("string").as("__dwca_merge_value"))
              .withColumn("__dwca_merge_producer_order", lit(producerOrder))
              .filter(
                  col("__dwca_merge_value")
                      .isNotNull()
                      .and(col("__dwca_merge_value").notEqual("")));
      contributions =
          contributions == null ? contribution : contributions.unionByName(contribution);
      producerOrder++;
    }

    if (contributions == null) {
      throw emptyMergeInvariant(extension, merge);
    }
    Column ordered =
        sort_array(
            collect_list(
                struct(
                    col("__dwca_merge_producer_order").as("producerOrder"),
                    col("__dwca_merge_value").as("value"))));
    return contributions
        .groupBy(COL_PARENT_KEY, COL_ROW_KEY)
        .agg(ordered.getItem(0).getField("value").as(targetAlias(merge.targetTerm())));
  }

  private Dataset<Row> materializeExtensionTargetMerge(
      TableLoader loader, CompiledExtension extension, CompiledTargetMerge merge) {
    if (merge.aggregation() instanceof ValueAggregation.FirstNonNull) {
      return materializeExtensionFirstNonNullMerge(loader, extension, merge);
    }

    Dataset<Row> contributions = null;
    int producerOrder = 0;

    for (CompiledTargetProducer producer : merge.producers()) {
      CompiledFragment fragment =
          extension.fragments().stream()
              .filter(candidate -> candidate.name().equals(producer.owner()))
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Merged extension producer references unknown fragment: "
                              + producer.owner()));

      Mapping mapping =
          new Mapping(
              "extension-merge:" + fragment.name(),
              fragment.sourceResource(),
              fragment.relations().stream().map(r -> r.toRelationStep()).toList(),
              List.of(),
              Projection.none());
      SparkPathResult pathResult = pathExecutor.execute(loader, mapping).pathResult();
      FieldRef parentKey = fragment.scopeKey();
      FieldRef rowKey =
          fragment.rowIdentity().orElseGet(() -> fragment.rowMatch().orElse(parentKey));

      Dataset<Row> contribution =
          pathResult
              .dataset()
              .select(
                  pathResult.columnOrNull(parentKey).cast("string").as(COL_PARENT_KEY),
                  pathResult.columnOrNull(rowKey).cast("string").as(COL_ROW_KEY),
                  rowExpression(producer, pathResult).cast("string").as("__dwca_merge_value"),
                  producer
                      .contributionIdentity()
                      .map(source -> pathResult.columnOrNull(source.field()).cast("string"))
                      .orElse(lit(null).cast("string"))
                      .as("__dwca_merge_identity"),
                  producer
                      .orderBy()
                      .map(source -> pathResult.columnOrNull(source.field()).cast("string"))
                      .orElse(lit(null).cast("string"))
                      .as("__dwca_merge_order"),
                  lit(producerOrder).as("__dwca_merge_producer_order"))
              .filter(
                  col("__dwca_merge_value")
                      .isNotNull()
                      .and(col("__dwca_merge_value").notEqual("")));
      contributions =
          contributions == null ? contribution : contributions.unionByName(contribution);
      producerOrder++;
    }

    if (contributions == null) {
      throw emptyMergeInvariant(extension, merge);
    }

    String alias = targetAlias(merge.targetTerm());
    boolean anyIdentity =
        merge.producers().stream().anyMatch(p -> p.contributionIdentity().isPresent());
    boolean allIdentity =
        merge.producers().stream().allMatch(p -> p.contributionIdentity().isPresent());
    if (anyIdentity && !allIdentity) {
      throw new IllegalArgumentException(
          "Merged extension target mixes identified and unidentified contributions: "
              + merge.targetTerm());
    }
    if (allIdentity) {
      contributions =
          contributions.dropDuplicates(
              COL_PARENT_KEY, COL_ROW_KEY, "__dwca_merge_identity", "__dwca_merge_value");
    }

    if (merge.aggregation() instanceof ValueAggregation.Delimited delimited) {
      boolean anyOrdered = merge.producers().stream().anyMatch(p -> p.orderBy().isPresent());
      boolean allOrdered = merge.producers().stream().allMatch(p -> p.orderBy().isPresent());
      if (anyOrdered && !allOrdered) {
        throw new IllegalArgumentException(
            "Merged extension target mixes ordered and unordered producers: " + merge.targetTerm());
      }
      if (allOrdered) {
        Column ordered =
            sort_array(
                collect_list(
                    org.apache.spark.sql.functions.struct(
                        col("__dwca_merge_order").as("order"),
                        col("__dwca_merge_value").as("value"))));
        Column values =
            org.apache.spark.sql.functions.transform(ordered, entry -> entry.getField("value"));
        values = org.apache.spark.sql.functions.filter(values, Column::isNotNull);
        if (delimited.distinct()) {
          values = array_distinct(values);
        }
        return contributions
            .groupBy(COL_PARENT_KEY, COL_ROW_KEY)
            .agg(
                org.apache.spark.sql.functions.array_join(values, delimited.delimiter()).as(alias));
      }

      Column values = collect_list(col("__dwca_merge_value"));
      if (delimited.distinct()) {
        values = array_distinct(values);
      }
      return contributions
          .groupBy(COL_PARENT_KEY, COL_ROW_KEY)
          .agg(concat_ws(delimited.delimiter(), sort_array(values)).as(alias));
    }
    throw new UnsupportedOperationException(
        "Unsupported extension target merge aggregation: "
            + merge.targetTerm()
            + " / "
            + merge.aggregation());
  }

  private static IllegalStateException emptyMergeInvariant(
      CompiledExtension extension, CompiledTargetMerge merge) {
    String fragments =
        extension.fragments().stream()
            .map(
                fragment ->
                    fragment.name()
                        + "[targets="
                        + fragment.targets().stream()
                            .map(CompiledTargetProducer::targetTerm)
                            .distinct()
                            .sorted()
                            .toList()
                        + "]")
            .toList()
            .toString();
    return new IllegalStateException(
        "Compiler invariant violated while materializing an extension target merge. "
            + "A compiled merge must contain at least one producer. "
            + "extensionRowType="
            + extension.rowType()
            + ", targetTerm="
            + merge.targetTerm()
            + ", aggregation="
            + merge.aggregation()
            + ", producerCount="
            + merge.producers().size()
            + ", fragments="
            + fragments);
  }

  private static IllegalStateException duplicateTargetException(
      String target, MaterializedTarget existing, MaterializedTarget incoming) {
    return new IllegalStateException(
        "Compiler invariant violated: duplicate target producer reached Spark materialization\n\n"
            + "Target:\n  "
            + target
            + "\n\n"
            + "Existing producer:\n"
            + indent(existing.describe(), "  ")
            + "\n\n"
            + "Incoming producer:\n"
            + indent(incoming.describe(), "  ")
            + "\n\n"
            + "Target ownership must be resolved by MappingCompiler before Spark execution.");
  }

  private static String indent(String text, String indent) {
    return indent + text.replace("\n", "\n" + indent);
  }

  private static String targetAlias(String term) {
    return "__dwca_term__" + shortHash(term);
  }

  private static String targetAlias(String term, String owner, Set<String> mergeTerms) {
    return mergeTerms.contains(term)
        ? "__dwca_term_contribution__" + shortHash(term + "|" + owner)
        : targetAlias(term);
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
