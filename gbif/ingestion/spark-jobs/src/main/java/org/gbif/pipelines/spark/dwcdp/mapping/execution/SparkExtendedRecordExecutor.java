package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.array_join;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.filter;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sort_array;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledCoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledTargetMerge;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledTargetProducer;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CardinalityStrategy;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.Mapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.Projection;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationStep;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaResource;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * First end-to-end compiler from a {@link MappingPlan} to {@link ExtendedRecord}.
 *
 * <p>This slice intentionally supports direct core fields plus extensions whose source resource is
 * either the core resource itself or one direct schema relation away. Longer/ambiguous attachment
 * paths will be added explicitly rather than guessed.
 */
public final class SparkExtendedRecordExecutor {
  private static final String CORE_ID = "__dwca_core_id";

  private final SchemaGraph graph;
  private final SparkExtensionMaterializer extensionMaterializer;
  private final SparkMappingPathExecutor pathExecutor;

  public SparkExtendedRecordExecutor(SchemaGraph graph) {
    this(graph, new ExecutionMetricsCollector());
  }

  public SparkExtendedRecordExecutor(
      SchemaGraph graph, ExecutionMetricsCollector metricsCollector) {
    this(graph, metricsCollector, SparkPathPrefixCache.disabled());
  }

  public SparkExtendedRecordExecutor(
      SchemaGraph graph,
      ExecutionMetricsCollector metricsCollector,
      SparkPathPrefixCache prefixCache) {
    this.graph = graph;
    this.extensionMaterializer =
        new SparkExtensionMaterializer(graph, metricsCollector, prefixCache);
    this.pathExecutor = new SparkMappingPathExecutor(graph, metricsCollector, prefixCache);
  }

  public Dataset<ExtendedRecord> execute(TableLoader loader, MappingPlan plan) {
    return execute(loader, new MappingCompiler(graph).compile(plan));
  }

  public Dataset<ExtendedRecord> execute(TableLoader loader, CompiledMapping plan) {
    SchemaResource coreResource =
        graph
            .resource(plan.coreSourceResource())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Unknown core resource: " + plan.coreSourceResource()));
    String corePk =
        coreResource
            .primaryKey()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Core resource has no primary key: " + plan.coreSourceResource()));
    Dataset<Row> rawCore =
        loader
            .load(plan.coreSourceResource())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Core resource is absent: " + plan.coreSourceResource()));
    requireColumn(rawCore, corePk, "core primary key");

    CoreProjection coreProjection = projectCore(loader, rawCore, plan);
    Dataset<Row> assembled = coreProjection.dataset();

    Map<String, ExtensionColumns> extensionColumns = new LinkedHashMap<>();
    for (CompiledExtension extension : plan.extensions()) {
      if (extension.fragments().isEmpty()) {
        continue;
      }
      boolean anySourcePresent =
          extension.fragments().stream()
              .anyMatch(fragment -> loader.load(fragment.sourceResource()).isPresent());
      if (!anySourcePresent) {
        // Extension roots are optional. For UNION composition any one branch can contribute.
        continue;
      }

      ExtensionMaterializationResult materialized =
          extensionMaterializer.materialize(loader, extension);
      if (materialized.targetColumns().isEmpty()) {
        continue;
      }
      String attachmentSourceResource = materialized.parentKeySource().path().rootResource();
      Dataset<Row> bridge =
          attachmentBridge(
              loader, plan, attachmentSourceResource, materialized.parentKeySource(), corePk);
      Dataset<Row> attached =
          bridge
              .join(
                  materialized.dataset(),
                  bridge
                      .col("__dwca_source_pk")
                      .equalTo(materialized.dataset().col(materialized.parentKeyColumn())),
                  "inner")
              .drop(materialized.dataset().col(materialized.parentKeyColumn()));

      List<TermColumn> terms =
          materialized.targetColumns().entrySet().stream()
              .sorted(Map.Entry.comparingByKey())
              .map(e -> new TermColumn(e.getKey(), e.getValue()))
              .toList();
      Column[] rowFields =
          terms.stream().map(e -> attached.col(e.column()).as(e.column())).toArray(Column[]::new);
      String extensionAlias = extensionAlias(extension.rowType());
      Dataset<Row> grouped =
          attached
              .select(col(CORE_ID), struct(rowFields).as("__dwca_extension_row"))
              .groupBy(CORE_ID)
              .agg(collect_list(col("__dwca_extension_row")).as(extensionAlias));

      assembled =
          assembled
              .join(grouped, assembled.col(CORE_ID).equalTo(grouped.col(CORE_ID)), "left_outer")
              .drop(grouped.col(CORE_ID));
      extensionColumns.put(extension.rowType(), new ExtensionColumns(extensionAlias, terms));
    }

    String coreRowType = coreRowType(plan.coreType());
    Map<String, String> coreTargetColumns = coreProjection.targetColumns();

    return assembled
        .map(
            (MapFunction<Row, ExtendedRecord>)
                row -> {
                  String id = row.getAs(CORE_ID);
                  if (id == null || id.isBlank()) {
                    return null;
                  }

                  Map<String, String> coreTerms = new HashMap<>();
                  for (Map.Entry<String, String> target : coreTargetColumns.entrySet()) {
                    Object value = row.getAs(target.getValue());
                    if (value != null) {
                      coreTerms.put(target.getKey(), termValue(value));
                    }
                  }

                  Map<String, List<Map<String, String>>> extensions = new HashMap<>();
                  for (Map.Entry<String, ExtensionColumns> extension :
                      extensionColumns.entrySet()) {
                    int index = row.fieldIndex(extension.getValue().arrayColumn());
                    if (row.isNullAt(index)) {
                      continue;
                    }
                    List<Row> extensionRows = row.getList(index);
                    List<Map<String, String>> mappedRows = new ArrayList<>();
                    for (Row extensionRow : extensionRows) {
                      Map<String, String> mapped = new HashMap<>();
                      for (TermColumn target : extension.getValue().terms()) {
                        Object value = extensionRow.getAs(target.column());
                        if (value != null) {
                          mapped.put(target.term(), termValue(value));
                        }
                      }
                      if (!mapped.isEmpty()) {
                        mappedRows.add(mapped);
                      }
                    }
                    if (!mappedRows.isEmpty()) {
                      extensions.put(extension.getKey(), mappedRows);
                    }
                  }

                  return ExtendedRecord.newBuilder()
                      .setId(id)
                      .setCoreId(null)
                      .setCoreRowType(coreRowType)
                      .setCoreTerms(coreTerms)
                      .setExtensions(extensions)
                      .build();
                },
            Encoders.bean(ExtendedRecord.class))
        .filter((FilterFunction<ExtendedRecord>) record -> record != null);
  }

  private CoreProjection projectCore(
      TableLoader loader, Dataset<Row> rawCore, CompiledMapping plan) {
    SchemaResource coreResource = graph.resource(plan.coreSourceResource()).orElseThrow();
    String corePk = coreResource.primaryKey().orElseThrow();

    List<Column> selected = new ArrayList<>();
    selected.add(
        coreIdentityColumn(rawCore, plan.coreType(), coreIdColumn(plan.coreType()), corePk)
            .as(CORE_ID));
    selected.add(rawCore.col(corePk).cast("string").as("__dwca_core_pk"));
    Map<String, String> targetColumns = new LinkedHashMap<>();

    Set<String> mergedTargets =
        plan.coreTargetMerges().stream()
            .map(CompiledTargetMerge::targetTerm)
            .collect(Collectors.toSet());

    for (CompiledTargetProducer field : plan.coreTargets()) {
      if (mergedTargets.contains(field.targetTerm())) {
        continue;
      }
      String alias = targetAlias(field.targetTerm());
      targetColumns.put(field.targetTerm(), alias);
      selected.add(coreTargetExpression(field, rawCore).as(alias));
    }

    Dataset<Row> assembled = rawCore.select(selected.toArray(Column[]::new));

    for (CompiledCoreFragment fragment : plan.coreFragments()) {
      if (fragment.targets().isEmpty()) {
        continue;
      }
      if (!fragment.sourceResource().equals(plan.coreSourceResource())) {
        throw new UnsupportedOperationException(
            "Core fragment must currently start at the core resource: " + fragment.name());
      }
      boolean hasNonMergedTarget =
          fragment.targets().stream()
              .anyMatch(target -> !mergedTargets.contains(target.targetTerm()));
      if (!hasNonMergedTarget) {
        continue;
      }

      Mapping mapping =
          new Mapping(
              "core-fragment:" + fragment.name(),
              fragment.sourceResource(),
              fragment.relations().stream().map(r -> r.toRelationStep()).toList(),
              List.of(),
              Projection.none());
      TableLoader fragmentLoader =
          resource ->
              resource.equals(plan.coreSourceResource())
                  ? Optional.of(rawCore)
                  : loader.load(resource);
      SparkPathResult pathResult = pathExecutor.execute(fragmentLoader, mapping).pathResult();
      String corePkAlias =
          pathResult.columnName(SchemaPath.root(plan.coreSourceResource()).field(corePk));

      List<Column> fragmentColumns = new ArrayList<>();
      fragmentColumns.add(
          pathResult.dataset().col(corePkAlias).cast("string").as("__dwca_fragment_core_pk"));
      for (CompiledTargetProducer target : fragment.targets()) {
        if (mergedTargets.contains(target.targetTerm())) {
          continue;
        }
        String alias = targetAlias(target.targetTerm());
        targetColumns.put(target.targetTerm(), alias);
        fragmentColumns.add(coreTargetExpression(target, pathResult).as(alias));
      }

      if (fragmentColumns.size() > 1) {
        Dataset<Row> projected =
            pathResult.dataset().select(fragmentColumns.toArray(Column[]::new));
        assembled =
            assembled
                .join(
                    projected,
                    assembled
                        .col("__dwca_core_pk")
                        .equalTo(projected.col("__dwca_fragment_core_pk")),
                    "left_outer")
                .drop(projected.col("__dwca_fragment_core_pk"));
      }
    }

    for (CompiledTargetMerge merge : plan.coreTargetMerges()) {
      Dataset<Row> merged = materializeCoreTargetMerge(loader, rawCore, plan, merge, corePk);
      if (merged == null) {
        continue;
      }
      String alias = targetAlias(merge.targetTerm());
      targetColumns.put(merge.targetTerm(), alias);
      assembled =
          assembled
              .join(
                  merged,
                  assembled.col("__dwca_core_pk").equalTo(merged.col("__dwca_merge_core_pk")),
                  "left_outer")
              .drop(merged.col("__dwca_merge_core_pk"));
    }

    return new CoreProjection(assembled, targetColumns);
  }

  private Dataset<Row> materializeCoreFirstNonNullMerge(
      TableLoader loader,
      Dataset<Row> rawCore,
      CompiledMapping plan,
      CompiledTargetMerge merge,
      String corePk) {
    Dataset<Row> contributions = null;
    int producerOrder = 0;

    for (CompiledTargetProducer producer : merge.producers()) {
      Dataset<Row> contribution;
      if (producer.owner().equals("core")) {
        contribution =
            rawCore
                .groupBy(rawCore.col(corePk).cast("string").as("__dwca_merge_core_pk"))
                .agg(
                    coreAggregateExpression(producer, rawCore)
                        .cast("string")
                        .as("__dwca_merge_value"));
      } else {
        CompiledCoreFragment fragment =
            plan.coreFragments().stream()
                .filter(candidate -> candidate.name().equals(producer.owner()))
                .findFirst()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Merged core producer references unknown fragment: "
                                + producer.owner()));
        Mapping mapping =
            new Mapping(
                "core-first-non-null-merge:" + fragment.name(),
                fragment.sourceResource(),
                fragment.relations().stream().map(r -> r.toRelationStep()).toList(),
                List.of(),
                Projection.none());
        TableLoader fragmentLoader =
            resource ->
                resource.equals(plan.coreSourceResource())
                    ? Optional.of(rawCore)
                    : loader.load(resource);
        SparkPathResult pathResult = pathExecutor.execute(fragmentLoader, mapping).pathResult();
        String corePkAlias =
            pathResult.columnName(SchemaPath.root(plan.coreSourceResource()).field(corePk));
        contribution =
            pathResult
                .dataset()
                .groupBy(
                    pathResult.dataset().col(corePkAlias).cast("string").as("__dwca_merge_core_pk"))
                .agg(
                    coreAggregateExpression(producer, pathResult)
                        .cast("string")
                        .as("__dwca_merge_value"));
      }
      contribution =
          contribution
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
      return null;
    }
    Column ordered =
        sort_array(
            collect_list(
                struct(
                    col("__dwca_merge_producer_order").as("producerOrder"),
                    col("__dwca_merge_value").as("value"))));
    return contributions
        .groupBy("__dwca_merge_core_pk")
        .agg(ordered.getItem(0).getField("value").as(targetAlias(merge.targetTerm())));
  }

  private Column coreAggregateExpression(CompiledTargetProducer target, Dataset<Row> root) {
    List<Column> sources =
        target.sources().stream()
            .map(source -> columnOrNull(root, source.field()).cast("string"))
            .toList();
    return coreAggregateExpression(
        target,
        sources,
        target
            .contributionIdentity()
            .map(source -> columnOrNull(root, source.field()).cast("string")),
        target.orderBy().map(source -> columnOrNull(root, source.field()).cast("string")));
  }

  private Column coreAggregateExpression(
      CompiledTargetProducer target, SparkPathResult pathResult) {
    List<Column> sources =
        target.sources().stream()
            .map(source -> pathResult.columnOrNull(source.field()).cast("string"))
            .toList();
    return coreAggregateExpression(
        target,
        sources,
        target
            .contributionIdentity()
            .map(source -> pathResult.columnOrNull(source.field()).cast("string")),
        target.orderBy().map(source -> pathResult.columnOrNull(source.field()).cast("string")));
  }

  private Column coreAggregateExpression(
      CompiledTargetProducer target,
      List<Column> sources,
      Optional<Column> contributionIdentity,
      Optional<Column> orderBy) {
    return SparkTargetExpression.aggregate(target, sources, contributionIdentity, orderBy);
  }

  private Dataset<Row> materializeCoreTargetMerge(
      TableLoader loader,
      Dataset<Row> rawCore,
      CompiledMapping plan,
      CompiledTargetMerge merge,
      String corePk) {
    if (merge.aggregation() instanceof ValueAggregation.FirstNonNull) {
      return materializeCoreFirstNonNullMerge(loader, rawCore, plan, merge, corePk);
    }

    Dataset<Row> contributions = null;
    int producerOrder = 0;

    for (CompiledTargetProducer producer : merge.producers()) {
      Dataset<Row> contribution;
      if (producer.owner().equals("core")) {
        contribution =
            rawCore.select(
                rawCore.col(corePk).cast("string").as("__dwca_merge_core_pk"),
                coreTargetExpression(producer, rawCore).cast("string").as("__dwca_merge_value"),
                producer
                    .contributionIdentity()
                    .map(source -> columnOrNull(rawCore, source.field()).cast("string"))
                    .orElse(lit(null).cast("string"))
                    .as("__dwca_merge_identity"),
                producer
                    .orderBy()
                    .map(source -> columnOrNull(rawCore, source.field()).cast("string"))
                    .orElse(lit(null).cast("string"))
                    .as("__dwca_merge_order"),
                lit(producerOrder).as("__dwca_merge_producer_order"));
      } else {
        CompiledCoreFragment fragment =
            plan.coreFragments().stream()
                .filter(candidate -> candidate.name().equals(producer.owner()))
                .findFirst()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Merged core producer references unknown fragment: "
                                + producer.owner()));
        Mapping mapping =
            new Mapping(
                "core-merge:" + fragment.name(),
                fragment.sourceResource(),
                fragment.relations().stream().map(r -> r.toRelationStep()).toList(),
                List.of(),
                Projection.none());
        TableLoader fragmentLoader =
            resource ->
                resource.equals(plan.coreSourceResource())
                    ? Optional.of(rawCore)
                    : loader.load(resource);
        SparkPathResult pathResult = pathExecutor.execute(fragmentLoader, mapping).pathResult();
        String corePkAlias =
            pathResult.columnName(SchemaPath.root(plan.coreSourceResource()).field(corePk));
        contribution =
            pathResult
                .dataset()
                .select(
                    pathResult.dataset().col(corePkAlias).cast("string").as("__dwca_merge_core_pk"),
                    coreTargetExpression(producer, pathResult)
                        .cast("string")
                        .as("__dwca_merge_value"),
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
                    lit(producerOrder).as("__dwca_merge_producer_order"));
      }
      contribution =
          contribution.filter(
              col("__dwca_merge_value").isNotNull().and(col("__dwca_merge_value").notEqual("")));
      contributions =
          contributions == null ? contribution : contributions.unionByName(contribution);
      producerOrder++;
    }

    if (contributions == null) {
      return null;
    }

    if (merge.aggregation() instanceof ValueAggregation.Delimited delimited) {
      boolean anyOrdered = merge.producers().stream().anyMatch(p -> p.orderBy().isPresent());
      boolean allOrdered = merge.producers().stream().allMatch(p -> p.orderBy().isPresent());
      if (anyOrdered && !allOrdered) {
        throw new IllegalArgumentException(
            "Merged target mixes ordered and unordered producers: " + merge.targetTerm());
      }

      boolean anyIdentity =
          merge.producers().stream().anyMatch(p -> p.contributionIdentity().isPresent());
      boolean allIdentity =
          merge.producers().stream().allMatch(p -> p.contributionIdentity().isPresent());
      if (anyIdentity && !allIdentity) {
        throw new IllegalArgumentException(
            "Merged target mixes identified and unidentified contributions: " + merge.targetTerm());
      }
      if (allIdentity) {
        contributions =
            contributions.dropDuplicates(
                "__dwca_merge_core_pk", "__dwca_merge_identity", "__dwca_merge_value");
      }

      if (allOrdered) {
        Column ordered =
            sort_array(
                collect_list(
                    struct(
                        col("__dwca_merge_order").as("order"),
                        col("__dwca_merge_value").as("value"))));
        Column values = transform(ordered, entry -> entry.getField("value"));
        values = filter(values, Column::isNotNull);
        if (delimited.distinct()) {
          values = array_distinct(values);
        }
        return contributions
            .groupBy("__dwca_merge_core_pk")
            .agg(array_join(values, delimited.delimiter()).as(targetAlias(merge.targetTerm())));
      }

      Column values = collect_list(col("__dwca_merge_value"));
      if (delimited.distinct()) {
        values = array_distinct(values);
      }
      return contributions
          .groupBy("__dwca_merge_core_pk")
          .agg(
              concat_ws(delimited.delimiter(), sort_array(values))
                  .as(targetAlias(merge.targetTerm())));
    }
    throw new UnsupportedOperationException(
        "Unsupported core target merge aggregation: "
            + merge.targetTerm()
            + " / "
            + merge.aggregation());
  }

  private static Column columnOrNull(Dataset<Row> dataset, FieldRef field) {
    return hasColumn(dataset, field.column())
        ? dataset.col(field.column())
        : lit(null).cast("string");
  }

  private Column coreTargetExpression(CompiledTargetProducer target, Dataset<Row> root) {
    List<Column> sources =
        target.sources().stream()
            .map(
                source ->
                    hasColumn(root, source.field().column())
                        ? root.col(source.field().column()).cast("string")
                        : lit(null).cast("string"))
            .toList();
    return combineCoreSources(target, sources);
  }

  private Column coreTargetExpression(CompiledTargetProducer target, SparkPathResult pathResult) {
    List<Column> sources =
        target.sources().stream()
            .map(source -> pathResult.columnOrNull(source.field()).cast("string"))
            .toList();
    return combineCoreSources(target, sources);
  }

  private static boolean hasColumn(Dataset<Row> dataset, String column) {
    for (String candidate : dataset.columns()) {
      if (candidate.equals(column)) {
        return true;
      }
    }
    return false;
  }

  private static Column combineCoreSources(CompiledTargetProducer target, List<Column> sources) {
    return SparkTargetExpression.row(target, sources);
  }

  private Dataset<Row> attachmentBridge(
      TableLoader loader,
      CompiledMapping plan,
      String sourceResource,
      FieldRef sourceScopeKey,
      String corePk) {
    String naturalId = coreIdColumn(plan.coreType());
    SchemaResource source =
        graph
            .resource(sourceResource)
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown extension source: " + sourceResource));

    if (!sourceScopeKey.path().rootResource().equals(sourceResource)
        || !sourceScopeKey.path().relations().isEmpty()) {
      throw new UnsupportedOperationException(
          "Extension scope key must currently be a field on the fragment root resource: "
              + sourceScopeKey.qualifiedName());
    }

    if (sourceResource.equals(plan.coreSourceResource())) {
      Dataset<Row> core = loader.load(sourceResource).orElseThrow();
      return core.select(
          coreIdentityColumn(core, plan.coreType(), naturalId, corePk).as(CORE_ID),
          core.col(sourceScopeKey.column()).cast("string").as("__dwca_source_pk"));
    }

    // A keyless child table can scope itself directly by its FK to the core, e.g.
    // event-identifier.event_fk -> event.event_pk. In that case there is no source PK to bridge
    // through: the materialized parent key already has the same value domain as the core PK.
    List<SchemaRelation> directScopeRelations =
        graph.relations(sourceResource, plan.coreSourceResource()).stream()
            .filter(relation -> relation.sourceColumn().equals(sourceScopeKey.column()))
            .filter(relation -> relation.targetColumn().equals(corePk))
            .toList();
    if (directScopeRelations.size() == 1) {
      Dataset<Row> core = loader.load(plan.coreSourceResource()).orElseThrow();
      return core.select(
          coreIdentityColumn(core, plan.coreType(), naturalId, corePk).as(CORE_ID),
          core.col(corePk).cast("string").as("__dwca_source_pk"));
    }
    if (directScopeRelations.size() > 1) {
      throw new IllegalArgumentException(
          "Ambiguous direct extension scope relation for " + sourceScopeKey.qualifiedName());
    }

    String sourcePk =
        source
            .primaryKey()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Extension source has no primary key and its scope key does not reference the core directly: "
                            + sourceResource
                            + "."
                            + sourceScopeKey.column()));
    if (!sourcePk.equals(sourceScopeKey.column())) {
      throw new UnsupportedOperationException(
          "Non-primary source scope keys are currently supported only when they reference the core directly: "
              + sourceScopeKey.qualifiedName());
    }

    SchemaRelation attachment = graph.resolve(plan.coreSourceResource(), sourceResource);
    RelationStep step = RelationStep.inferred(sourceResource).with(CardinalityStrategy.fanOut());
    Mapping mapping =
        new Mapping(
            "attach:" + plan.coreSourceResource() + "->" + sourceResource,
            plan.coreSourceResource(),
            List.of(step),
            List.of(),
            Projection.none());
    MappingExecutionResult execution = pathExecutor.execute(loader, mapping);
    if (!execution.completePath()) {
      SchemaPath corePath = SchemaPath.root(plan.coreSourceResource());
      return execution
          .pathResult()
          .dataset()
          .limit(0)
          .select(
              coreIdentityColumn(
                      execution.pathResult(), corePath, plan.coreType(), naturalId, corePk)
                  .as(CORE_ID),
              lit(null).cast("string").as("__dwca_source_pk"));
    }

    SchemaPath corePath = SchemaPath.root(plan.coreSourceResource());
    SchemaPath sourcePath = corePath.append(attachment);
    return execution
        .pathResult()
        .dataset()
        .select(
            coreIdentityColumn(execution.pathResult(), corePath, plan.coreType(), naturalId, corePk)
                .as(CORE_ID),
            execution
                .pathResult()
                .column(sourcePath.field(sourcePk))
                .cast("string")
                .as("__dwca_source_pk"));
  }

  private static Column coreIdentityColumn(
      Dataset<Row> dataset, CoreType coreType, String naturalId, String corePk) {
    Column primaryKey = dataset.col(corePk).cast("string");
    Column fallback = generatedCoreId(coreType, primaryKey);
    if (!hasColumn(dataset, naturalId)) {
      return fallback;
    }
    return naturalIdOrFallback(dataset.col(naturalId).cast("string"), fallback);
  }

  private static Column coreIdentityColumn(
      SparkPathResult pathResult,
      SchemaPath corePath,
      CoreType coreType,
      String naturalId,
      String corePk) {
    Column primaryKey = pathResult.column(corePath.field(corePk)).cast("string");
    Column fallback = generatedCoreId(coreType, primaryKey);
    Column natural = pathResult.columnOrNull(corePath.field(naturalId)).cast("string");
    return naturalIdOrFallback(natural, fallback);
  }

  private static Column naturalIdOrFallback(Column naturalId, Column fallback) {
    return when(naturalId.isNotNull().and(length(trim(naturalId)).gt(0)), naturalId)
        .otherwise(fallback);
  }

  private static Column generatedCoreId(CoreType coreType, Column primaryKey) {
    String prefix =
        switch (coreType) {
          case EVENT -> "urn:gbif:dwcdp:event:";
          case OCCURRENCE -> "urn:gbif:dwcdp:occurrence:";
        };
    return concat(lit(prefix), primaryKey);
  }

  private static String coreIdColumn(CoreType coreType) {
    return switch (coreType) {
      case EVENT -> "eventID";
      case OCCURRENCE -> "occurrenceID";
    };
  }

  private static String coreRowType(CoreType coreType) {
    return switch (coreType) {
      case EVENT -> DwcTerm.Event.qualifiedName();
      case OCCURRENCE -> DwcTerm.Occurrence.qualifiedName();
    };
  }

  private static void requireColumn(Dataset<Row> dataset, String column, String purpose) {
    for (String candidate : dataset.columns()) {
      if (candidate.equals(column)) {
        return;
      }
    }
    throw new IllegalArgumentException("Missing " + purpose + " column: " + column);
  }

  private static String targetAlias(String term) {
    return "__dwca_core_term__" + shortHash(term);
  }

  private static String extensionAlias(String rowType) {
    return "__dwca_extension__" + shortHash(rowType);
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

  /** Converts a materialized scalar term value to the textual ExtendedRecord representation. */
  private static String termValue(Object value) {
    return value instanceof String stringValue ? stringValue : String.valueOf(value);
  }

  private record CoreProjection(Dataset<Row> dataset, Map<String, String> targetColumns) {}

  private record TermColumn(String term, String column) implements Serializable {}

  private record ExtensionColumns(String arrayColumn, List<TermColumn> terms)
      implements Serializable {}
}
