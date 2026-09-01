package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lit;

import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.NestedContextDiscoveryFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.NestedExtensionContext;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.util.TableLoader;

/** Executes declarative nested-scope discovery into opaque parent/row/context identities. */
final class SparkNestedContextDiscovery {
  static final String COL_PARENT = "__dwcdp_nested_parent";
  static final String COL_ROW = "__dwcdp_nested_row";
  static final String COL_CONTEXT = "__dwcdp_nested_context";

  private final SparkSchemaPathExecutor pathExecutor;
  private final NestedExtensionContext context;

  SparkNestedContextDiscovery(SchemaGraph graph, NestedExtensionContext context) {
    this.pathExecutor = new SparkSchemaPathExecutor(graph);
    this.context = context;
  }

  Result discover(TableLoader loader) {
    Dataset<Row> discovered = null;
    for (NestedContextDiscoveryFragment fragment : context.discoveryFragments()) {
      if (!isExecutable(loader, fragment)) {
        continue;
      }
      SparkPathResult result = pathExecutor.execute(loader, fragment.path().schemaPath());
      Dataset<Row> branch =
          result
              .dataset()
              .select(
                  result.column(fragment.parentIdentity()).cast("string").as(COL_PARENT),
                  result.column(fragment.rowIdentity()).cast("string").as(COL_ROW),
                  fragment
                      .contextIdentity()
                      .map(result::columnOrNull)
                      .orElse(lit(null))
                      .cast("string")
                      .as(COL_CONTEXT))
              .filter(col(COL_PARENT).isNotNull().and(col(COL_ROW).isNotNull()));
      discovered = discovered == null ? branch : discovered.unionByName(branch);
    }

    if (discovered == null) {
      throw new IllegalStateException(
          "Nested context has no executable discovery fragments for " + context.extensionRowType());
    }

    Dataset<Row> identities = discovered.distinct();
    Dataset<Row> ownership = identities.select(COL_PARENT, COL_ROW).distinct();
    Dataset<Row> contextRows =
        identities
            .filter(col(COL_CONTEXT).isNotNull())
            .select(COL_PARENT, COL_ROW, COL_CONTEXT)
            .distinct();
    Dataset<Row> uniqueContext =
        contextRows
            .groupBy(COL_PARENT, COL_ROW)
            .agg(
                countDistinct(col(COL_CONTEXT)).as("__dwcdp_nested_context_count"),
                first(col(COL_CONTEXT), true).as(COL_CONTEXT))
            .filter(col("__dwcdp_nested_context_count").equalTo(1))
            .drop("__dwcdp_nested_context_count");

    return new Result(ownership, contextRows, uniqueContext);
  }

  private boolean isExecutable(TableLoader loader, NestedContextDiscoveryFragment fragment) {
    if (loader.load(fragment.path().rootResource()).isEmpty()
        || !fieldAvailable(loader, fragment.parentIdentity())
        || !fieldAvailable(loader, fragment.rowIdentity())
        || fragment.contextIdentity().filter(field -> !fieldAvailable(loader, field)).isPresent()) {
      return false;
    }

    return fragment.path().schemaPath().relations().stream()
        .allMatch(
            relation ->
                loader
                        .load(relation.sourceResource())
                        .filter(dataset -> hasColumn(dataset, relation.sourceColumn()))
                        .isPresent()
                    && loader
                        .load(relation.targetResource())
                        .filter(dataset -> hasColumn(dataset, relation.targetColumn()))
                        .isPresent());
  }

  private static boolean fieldAvailable(TableLoader loader, FieldRef field) {
    return loader
        .load(field.path().currentResource())
        .filter(dataset -> hasColumn(dataset, field.column()))
        .isPresent();
  }

  private static boolean hasColumn(Dataset<Row> dataset, String column) {
    return Arrays.asList(dataset.columns()).contains(column);
  }

  record Result(Dataset<Row> ownership, Dataset<Row> contextRows, Dataset<Row> uniqueContext) {}
}
