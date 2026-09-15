package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.NestedExtensionContext;
import org.gbif.pipelines.spark.util.TableLoader;

/** Builds isolated physical views for one declaratively configured nested extension context. */
final class SparkNestedContextLoader {
  private static final String CONTEXT_PREFIX = "urn:gbif:dwcdp:nested-context:";

  private SparkNestedContextLoader() {}

  static Optional<TableLoader> loader(
      TableLoader loader,
      NestedExtensionContext context,
      SparkNestedContextDiscovery.Result discovery) {
    Optional<Dataset<Row>> rows = loader.load(context.rowResource());
    if (rows.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> resolved = resolvedContext(discovery);
    Dataset<Row> nestedRows = nestedRows(rows.get(), context, resolved);
    Optional<Dataset<Row>> nestedContext =
        loader
            .load(context.contextResource())
            .map(contextual -> nestedContext(contextual, context, resolved));

    return Optional.of(
        resource -> {
          if (resource.equals(context.rowResource())) {
            return Optional.of(nestedRows);
          }
          if (resource.equals(context.contextResource()) && nestedContext.isPresent()) {
            return nestedContext;
          }
          return loader.load(resource);
        });
  }

  /**
   * Resolves one compact parent/row/context identity relation and deliberately truncates the Spark
   * lineage before contextual fragments consume it.
   *
   * <p>The left join preserves every discovered parent/row ownership. A unique context is attached
   * only when discovery resolved exactly one context identity; zero or ambiguous contexts remain
   * null. Eager local checkpointing is intentional here: downstream contextual fragments should
   * depend on this compact resolved relation, not inherit and replicate the complete discovery DAG.
   */
  static Dataset<Row> resolvedContext(SparkNestedContextDiscovery.Result discovery) {
    Dataset<Row> ownership = discovery.ownership().alias("own");
    Dataset<Row> uniqueContext = discovery.uniqueContext().alias("ctx");

    Column sameParent =
        col("own." + SparkNestedContextDiscovery.COL_PARENT)
            .equalTo(col("ctx." + SparkNestedContextDiscovery.COL_PARENT));
    Column sameRow =
        col("own." + SparkNestedContextDiscovery.COL_ROW)
            .equalTo(col("ctx." + SparkNestedContextDiscovery.COL_ROW));

    return ownership
        .join(uniqueContext, sameParent.and(sameRow), "left_outer")
        .select(
            col("own." + SparkNestedContextDiscovery.COL_PARENT)
                .as(SparkNestedContextDiscovery.COL_PARENT),
            col("own." + SparkNestedContextDiscovery.COL_ROW)
                .as(SparkNestedContextDiscovery.COL_ROW),
            col("ctx." + SparkNestedContextDiscovery.COL_CONTEXT)
                .as(SparkNestedContextDiscovery.COL_CONTEXT))
        .localCheckpoint(true);
  }

  private static Dataset<Row> nestedRows(
      Dataset<Row> rows, NestedExtensionContext context, Dataset<Row> resolved) {
    Dataset<Row> source = rows.alias("row");
    Dataset<Row> own = resolved.alias("own");
    Column syntheticLink = syntheticLink("own");
    String contextLinkColumn =
        SparkInternalColumns.nestedContextLink(
            context.rowContextLink().column(), context.contextRowLink().column());

    List<Column> selected = new ArrayList<>();
    for (String name : rows.columns()) {
      if (name.equals(context.rowParentKey().column())) {
        selected.add(col("own." + SparkNestedContextDiscovery.COL_PARENT).as(name));
      } else {
        selected.add(col("row." + name).as(name));
      }
    }
    selected.add(syntheticLink.as(contextLinkColumn));

    return own.join(
            source,
            col("own." + SparkNestedContextDiscovery.COL_ROW)
                .equalTo(col("row." + context.rowIdentity().column())),
            "inner")
        .select(selected.toArray(Column[]::new));
  }

  private static Dataset<Row> nestedContext(
      Dataset<Row> contextual, NestedExtensionContext context, Dataset<Row> resolved) {
    Dataset<Row> source = contextual.alias("ctxrow");
    Dataset<Row> ctx =
        resolved.filter(col(SparkNestedContextDiscovery.COL_CONTEXT).isNotNull()).alias("ctx");
    Column syntheticLink = syntheticLink("ctx");
    String contextLinkColumn =
        SparkInternalColumns.nestedContextLink(
            context.rowContextLink().column(), context.contextRowLink().column());

    List<Column> selected = new ArrayList<>();
    Arrays.stream(contextual.columns())
        .forEach(name -> selected.add(col("ctxrow." + name).as(name)));
    selected.add(syntheticLink.as(contextLinkColumn));

    return ctx.join(
            source,
            col("ctx." + SparkNestedContextDiscovery.COL_CONTEXT)
                .equalTo(col("ctxrow." + context.contextIdentity().column())),
            "inner")
        .select(selected.toArray(Column[]::new));
  }

  private static Column syntheticLink(String alias) {
    return concat(
        lit(CONTEXT_PREFIX),
        col(alias + "." + SparkNestedContextDiscovery.COL_PARENT),
        lit(":"),
        col(alias + "." + SparkNestedContextDiscovery.COL_ROW));
  }
}
