package org.gbif.pipelines.spark.dwcdp.mapping;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

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

    CoreProjection coreProjection = projectCore(rawCore, plan);
    Dataset<Row> assembled = coreProjection.dataset();

    Map<String, ExtensionColumns> extensionColumns = new LinkedHashMap<>();
    for (ExtensionMapping extension : plan.extensions()) {
      if (extension.fragments().isEmpty()) {
        continue;
      }
      ExtensionMaterializationResult materialized = extensionMaterializer.materialize(loader, extension);
      if (materialized.targetColumns().isEmpty()) {
        continue;
      }

      String sourceResource = extension.fragments().get(0).sourceResource();
      Dataset<Row> bridge = attachmentBridge(loader, plan, sourceResource, naturalId, corePk);
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
                String value = row.getAs(target.getValue());
                if (value != null) {
                  coreTerms.put(target.getKey(), value);
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
                    String value = extensionRow.getAs(target.column());
                    if (value != null) {
                      mapped.put(target.term(), value);
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

  private CoreProjection projectCore(Dataset<Row> rawCore, MappingPlan plan) {
    List<Column> selected = new ArrayList<>();
    selected.add(rawCore.col(coreIdColumn(plan.coreType())).cast("string").as(CORE_ID));
    Map<String, String> targetColumns = new LinkedHashMap<>();

    for (TargetFieldMapping field : plan.coreFields()) {
      for (FieldRef source : field.sources()) {
        if (!source.path().relations().isEmpty()
            || !source.path().rootResource().equals(plan.coreSourceResource())) {
          throw new UnsupportedOperationException(
              "Core-field path navigation is not part of this slice yet: " + source.qualifiedName());
        }
        requireColumn(rawCore, source.column(), "core field " + source.qualifiedName());
      }

      String alias = targetAlias(field.targetTerm());
      targetColumns.put(field.targetTerm(), alias);
      List<Column> sources = field.sources().stream().map(s -> rawCore.col(s.column()).cast("string")).toList();
      Column value;
      if (field.aggregation() instanceof ValueAggregation.FirstNonNull) {
        value = coalesce(sources.toArray(Column[]::new));
      } else if (field.aggregation() instanceof ValueAggregation.ExactlyOne && sources.size() == 1) {
        value = sources.get(0);
      } else {
        throw new UnsupportedOperationException(
            "Unsupported core-field aggregation in first ExtendedRecord slice: " + field.aggregation());
      }
      selected.add(value.as(alias));
    }

    return new CoreProjection(rawCore.select(selected.toArray(Column[]::new)), targetColumns);
  }

  private Dataset<Row> attachmentBridge(
      TableLoader loader,
      MappingPlan plan,
      String sourceResource,
      String naturalId,
      String corePk) {
    SchemaResource source =
        graph.resource(sourceResource)
            .orElseThrow(() -> new IllegalArgumentException("Unknown extension source: " + sourceResource));
    String sourcePk =
        source.primaryKey()
            .orElseThrow(
                () -> new IllegalArgumentException("Extension source has no primary key: " + sourceResource));

    if (sourceResource.equals(plan.coreSourceResource())) {
      Dataset<Row> core = loader.load(sourceResource).orElseThrow();
      return core.select(
          core.col(naturalId).cast("string").as(CORE_ID),
          core.col(corePk).cast("string").as("__dwca_source_pk"));
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

  private record CoreProjection(Dataset<Row> dataset, Map<String, String> targetColumns) {}

  private record TermColumn(String term, String column) implements java.io.Serializable {}

  private record ExtensionColumns(String arrayColumn, List<TermColumn> terms)
      implements java.io.Serializable {}
}
