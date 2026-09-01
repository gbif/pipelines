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
    Optional<Dataset<Row>> contextual = loader.load(context.contextResource());
    if (rows.isEmpty() || contextual.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> nestedRows = nestedRows(rows.get(), context, discovery.ownership());
    Dataset<Row> nestedContext =
        nestedContext(contextual.get(), context, discovery.uniqueContext());

    return Optional.of(
        resource -> {
          if (resource.equals(context.rowResource())) {
            return Optional.of(nestedRows);
          }
          if (resource.equals(context.contextResource())) {
            return Optional.of(nestedContext);
          }
          return loader.load(resource);
        });
  }

  private static Dataset<Row> nestedRows(
      Dataset<Row> rows, NestedExtensionContext context, Dataset<Row> ownership) {
    Dataset<Row> source = rows.alias("row");
    Dataset<Row> own = ownership.alias("own");
    Column syntheticLink = syntheticLink("own");

    List<Column> selected = new ArrayList<>();
    for (String name : rows.columns()) {
      if (name.equals(context.rowParentKey().column())) {
        selected.add(col("own." + SparkNestedContextDiscovery.COL_PARENT).as(name));
      } else if (name.equals(context.rowContextLink().column())) {
        selected.add(syntheticLink.as(name));
      } else {
        selected.add(col("row." + name).as(name));
      }
    }

    return own.join(
            source,
            col("own." + SparkNestedContextDiscovery.COL_ROW)
                .equalTo(col("row." + context.rowIdentity().column())),
            "inner")
        .select(selected.toArray(Column[]::new));
  }

  private static Dataset<Row> nestedContext(
      Dataset<Row> contextual, NestedExtensionContext context, Dataset<Row> uniqueContext) {
    Dataset<Row> source = contextual.alias("ctxrow");
    Dataset<Row> ctx = uniqueContext.alias("ctx");
    Column syntheticLink = syntheticLink("ctx");

    List<Column> selected = new ArrayList<>();
    Arrays.stream(contextual.columns())
        .forEach(
            name -> {
              if (name.equals(context.contextRowLink().column())) {
                selected.add(syntheticLink.as(name));
              } else {
                selected.add(col("ctxrow." + name).as(name));
              }
            });

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
