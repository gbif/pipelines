package org.gbif.pipelines.spark.dwcdp.mapping;

import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.array_join;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.filter;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sort_array;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.when;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledCoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledTargetMerge;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledTargetProducer;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingCompiler;

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
    this.graph = graph;
    this.extensionMaterializer = new SparkExtensionMaterializer(graph);
    this.pathExecutor = new SparkMappingPathExecutor(graph);
  }

  public Dataset<ExtendedRecord> execute(TableLoader loader, MappingPlan plan) {
    return execute(loader, new MappingCompiler(graph).compile(plan));
  }

  public Dataset<ExtendedRecord> execute(TableLoader loader, CompiledMapping plan) {
    SchemaResource coreResource =
        graph.resource(plan.coreSourceResource())
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown core resource: " + plan.coreSourceResource()));
    String corePk =
        coreResource
            .primaryKey()
            .orElseThrow(
                () -> new IllegalArgumentException("Core resource has no primary key: " + plan.coreSourceResource()));
    String naturalId = coreIdColumn(plan.coreType());

    Dataset<Row> rawCore =
        loader
            .load(plan.coreSourceResource())
            .orElseThrow(
                () -> new IllegalArgumentException("Core resource is absent: " + plan.coreSourceResource()));
    requireColumn(rawCore, naturalId, "core natural id");
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
      String attachmentSourceResource =
          materialized.parentKeySource().path().rootResource();
      Dataset<Row> bridge =
          attachmentBridge(
              loader,
              plan,
              attachmentSourceResource,
              materialized.parentKeySource(),
              naturalId,
              corePk);
      Dataset<Row> attached =
          bridge
              .join(
                  materialized.dataset(),
                  bridge.col("__dwca_source_pk")
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

    return assembled.map(
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
              for (Map.Entry<String, ExtensionColumns> extension : extensionColumns.entrySet()) {
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

  private CoreProjection projectCore(TableLoader loader, Dataset<Row> rawCore, CompiledMapping plan) {
    SchemaResource coreResource = graph.resource(plan.coreSourceResource()).orElseThrow();
    String corePk = coreResource.primaryKey().orElseThrow();

    List<Column> selected = new ArrayList<>();
    selected.add(rawCore.col(coreIdColumn(plan.coreType())).cast("string").as(CORE_ID));
    selected.add(rawCore.col(corePk).cast("string").as("__dwca_core_pk"));
    Map<String, String> targetColumns = new LinkedHashMap<>();

    java.util.Set<String> mergedTargets =
        plan.coreTargetMerges().stream()
            .map(CompiledTargetMerge::targetTerm)
            .collect(java.util.stream.Collectors.toSet());

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
                  ? java.util.Optional.of(rawCore)
                  : loader.load(resource);
      SparkPathResult pathResult = pathExecutor.execute(fragmentLoader, mapping).pathResult();
      String corePkAlias =
          pathResult.columnName(SchemaPath.root(plan.coreSourceResource()).field(corePk));

      List<Column> fragmentColumns = new ArrayList<>();
      fragmentColumns.add(pathResult.dataset().col(corePkAlias).cast("string").as("__dwca_fragment_core_pk"));
      for (CompiledTargetProducer target : fragment.targets()) {
        if (mergedTargets.contains(target.targetTerm())) {
          continue;
        }
        String alias = targetAlias(target.targetTerm());
        targetColumns.put(target.targetTerm(), alias);
        fragmentColumns.add(coreTargetExpression(target, pathResult).as(alias));
      }

      if (fragmentColumns.size() > 1) {
        Dataset<Row> projected = pathResult.dataset().select(fragmentColumns.toArray(Column[]::new));
        assembled =
            assembled
                .join(
                    projected,
                    assembled.col("__dwca_core_pk").equalTo(projected.col("__dwca_fragment_core_pk")),
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

  private Dataset<Row> materializeCoreTargetMerge(
      TableLoader loader,
      Dataset<Row> rawCore,
      CompiledMapping plan,
      CompiledTargetMerge merge,
      String corePk) {
    Dataset<Row> contributions = null;
    boolean anyOrdered = merge.producers().stream().anyMatch(p -> p.orderBy().isPresent());
    boolean allOrdered = merge.producers().stream().allMatch(p -> p.orderBy().isPresent());
    if (anyOrdered && !allOrdered) {
      throw new IllegalArgumentException(
          "Merged target mixes ordered and unordered producers: " + merge.targetTerm());
    }

    for (CompiledTargetProducer producer : merge.producers()) {
      Dataset<Row> contribution;
      if (producer.owner().equals("core")) {
        contribution =
            rawCore.select(
                rawCore.col(corePk).cast("string").as("__dwca_merge_core_pk"),
                coreTargetExpression(producer, rawCore).cast("string").as("__dwca_merge_value"),
                producer.contributionIdentity()
                    .map(source -> columnOrNull(rawCore, source.field()).cast("string"))
                    .orElse(lit(null).cast("string"))
                    .as("__dwca_merge_identity"),
                producer.orderBy()
                    .map(source -> columnOrNull(rawCore, source.field()).cast("string"))
                    .orElse(lit(null).cast("string"))
                    .as("__dwca_merge_order"));
      } else {
        CompiledCoreFragment fragment =
            plan.coreFragments().stream()
                .filter(candidate -> candidate.name().equals(producer.owner()))
                .findFirst()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Merged core producer references unknown fragment: " + producer.owner()));
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
                    ? java.util.Optional.of(rawCore)
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
                    producer.contributionIdentity()
                        .map(source -> pathResult.columnOrNull(source.field()).cast("string"))
                        .orElse(lit(null).cast("string"))
                        .as("__dwca_merge_identity"),
                    producer.orderBy()
                        .map(source -> pathResult.columnOrNull(source.field()).cast("string"))
                        .orElse(lit(null).cast("string"))
                        .as("__dwca_merge_order"));
      }
      contribution =
          contribution.filter(
              col("__dwca_merge_value").isNotNull().and(col("__dwca_merge_value").notEqual("")));
      contributions =
          contributions == null ? contribution : contributions.unionByName(contribution);
    }

    if (contributions == null) {
      return null;
    }

    boolean anyIdentity = merge.producers().stream().anyMatch(p -> p.contributionIdentity().isPresent());
    boolean allIdentity = merge.producers().stream().allMatch(p -> p.contributionIdentity().isPresent());
    if (anyIdentity && !allIdentity) {
      throw new IllegalArgumentException(
          "Merged target mixes identified and unidentified contributions: " + merge.targetTerm());
    }
    if (allIdentity) {
      contributions =
          contributions.dropDuplicates(
              "__dwca_merge_core_pk", "__dwca_merge_identity", "__dwca_merge_value");
    }

    if (merge.aggregation() instanceof ValueAggregation.Delimited delimited) {
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
        "Unsupported core target merge aggregation: " + merge.targetTerm() + " / " + merge.aggregation());
  }

  private static Column columnOrNull(Dataset<Row> dataset, FieldRef field) {
    return hasColumn(dataset, field.column())
        ? dataset.col(field.column())
        : lit(null).cast("string");
  }

  private Column coreTargetExpression(CompiledTargetProducer target, Dataset<Row> root) {
    if (target.aggregation() instanceof ValueAggregation.PresentOrFallback) {
      return target.sources().stream()
          .filter(source -> hasColumn(root, source.field().column()))
          .findFirst()
          .map(source -> root.col(source.field().column()).cast("string"))
          .orElse(lit(null).cast("string"));
    }
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
    if (target.aggregation() instanceof ValueAggregation.PresentOrFallback) {
      return target.sources().stream()
          .filter(source -> pathResult.aliases().containsKey(source.field()))
          .findFirst()
          .map(source -> pathResult.column(source.field()).cast("string"))
          .orElse(lit(null).cast("string"));
    }
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

  private static Column combineCoreSources(
      CompiledTargetProducer target, List<Column> sources) {
    if (target.sourceMode() == TargetFieldMapping.SourceMode.ONE_OF
        && target.aggregation() instanceof ValueAggregation.FirstNonNull) {
      return coalesce(sources.toArray(Column[]::new));
    }
    if (target.aggregation() instanceof ValueAggregation.ExactlyOne && sources.size() == 1) {
      return sources.get(0);
    }
    if (target.aggregation() instanceof ValueAggregation.LabeledOrFallback labeled) {
      if (sources.size() < 3) {
        throw new IllegalArgumentException(
            "LabeledOrFallback requires [label, name, fallback...] sources for " + target.targetTerm());
      }
      Column labeledValue =
          when(
                  sources.get(0).isNotNull().and(sources.get(1).isNotNull()),
                  concat(sources.get(0), lit(labeled.separator()), sources.get(1)))
              .otherwise(sources.get(2));
      if (sources.size() == 3) {
        return labeledValue;
      }
      List<Column> fallback = new ArrayList<>();
      fallback.add(labeledValue);
      fallback.addAll(sources.subList(3, sources.size()));
      return coalesce(fallback.toArray(Column[]::new));
    }
    if (target.aggregation() instanceof ValueAggregation.PreferredLabeledOrFallback labeled) {
      if (sources.size() < 4) {
        throw new IllegalArgumentException(
            "PreferredLabeledOrFallback requires [preferred, label, name, fallback...] sources for "
                + target.targetTerm());
      }
      Column labeledValue =
          when(
                  sources.get(1).isNotNull().and(sources.get(2).isNotNull()),
                  concat(sources.get(1), lit(labeled.separator()), sources.get(2)))
              .otherwise(sources.get(3));
      List<Column> values = new ArrayList<>();
      values.add(sources.get(0));
      values.add(labeledValue);
      values.addAll(sources.subList(4, sources.size()));
      return coalesce(values.toArray(Column[]::new));
    }
    throw new UnsupportedOperationException(
        "Unsupported core-field aggregation: " + target.targetTerm() + " / " + target.aggregation());
  }

  private Dataset<Row> attachmentBridge(
      TableLoader loader,
      CompiledMapping plan,
      String sourceResource,
      FieldRef sourceScopeKey,
      String naturalId,
      String corePk) {
    SchemaResource source =
        graph.resource(sourceResource)
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
          core.col(naturalId).cast("string").as(CORE_ID),
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
          core.col(naturalId).cast("string").as(CORE_ID),
          core.col(corePk).cast("string").as("__dwca_source_pk"));
    }
    if (directScopeRelations.size() > 1) {
      throw new IllegalArgumentException(
          "Ambiguous direct extension scope relation for " + sourceScopeKey.qualifiedName());
    }

    String sourcePk =
        source.primaryKey()
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
      return execution.pathResult().dataset().limit(0).select(
          execution.pathResult().column(corePath.field(naturalId)).cast("string").as(CORE_ID),
          lit(null).cast("string").as("__dwca_source_pk"));
    }

    SchemaPath corePath = SchemaPath.root(plan.coreSourceResource());
    SchemaPath sourcePath = corePath.append(attachment);
    return execution.pathResult().dataset().select(
        execution.pathResult().column(corePath.field(naturalId)).cast("string").as(CORE_ID),
        execution.pathResult().column(sourcePath.field(sourcePk)).cast("string").as("__dwca_source_pk"));
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

  /** Converts a materialized scalar term value to the textual ExtendedRecord representation. */
  private static String termValue(Object value) {
    return value instanceof String stringValue ? stringValue : String.valueOf(value);
  }

  private record CoreProjection(Dataset<Row> dataset, Map<String, String> targetColumns) {}

  private record TermColumn(String term, String column) implements java.io.Serializable {}

  private record ExtensionColumns(String arrayColumn, List<TermColumn> terms)
      implements java.io.Serializable {}
}
