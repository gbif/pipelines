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
import org.gbif.pipelines.spark.util.TableLoader;

/** Isolated occurrence/material views used only by Material-derived Event-occurrence enrichment. */
final class EventOccurrenceMaterialContext {
  private static final String CONTEXT_OCCURRENCE_PREFIX = "urn:gbif:dwcdp:context-occurrence:";

  private EventOccurrenceMaterialContext() {}

  static Optional<TableLoader> loader(
      TableLoader loader, SparkEventOccurrenceDiscovery.Result discovery) {
    Optional<Dataset<Row>> occurrence = loader.load("occurrence");
    Optional<Dataset<Row>> material = loader.load("material");
    if (occurrence.isEmpty() || material.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> contextualOccurrence =
        contextualOccurrence(occurrence.get(), discovery.ownership());
    Dataset<Row> contextualMaterial =
        contextualMaterial(material.get(), discovery.uniqueMaterialContext());

    return Optional.of(
        resource -> {
          if (resource.equals("occurrence")) {
            return Optional.of(contextualOccurrence);
          }
          if (resource.equals("material")) {
            return Optional.of(contextualMaterial);
          }
          return loader.load(resource);
        });
  }

  private static Dataset<Row> contextualOccurrence(
      Dataset<Row> occurrence, Dataset<Row> ownership) {
    Dataset<Row> o = occurrence.alias("o");
    Dataset<Row> own = ownership.alias("own");
    Column contextOccurrenceId = contextOccurrenceId("own");

    List<Column> selected = new ArrayList<>();
    for (String name : occurrence.columns()) {
      if (name.equals("event_fk")) {
        selected.add(col("own." + SparkEventOccurrenceDiscovery.COL_EVENT_PK).as(name));
      } else if (name.equals("occurrenceID")) {
        selected.add(contextOccurrenceId.as(name));
      } else {
        selected.add(col("o." + name).as(name));
      }
    }

    return own.join(
            o,
            col("own." + SparkEventOccurrenceDiscovery.COL_OCCURRENCE_PK)
                .equalTo(col("o.occurrence_pk")),
            "inner")
        .select(selected.toArray(Column[]::new));
  }

  private static Dataset<Row> contextualMaterial(
      Dataset<Row> material, Dataset<Row> uniqueMaterialContext) {
    Dataset<Row> m = material.alias("m");
    Dataset<Row> ctx = uniqueMaterialContext.alias("ctx");
    Column contextOccurrenceId = contextOccurrenceId("ctx");

    List<Column> selected = new ArrayList<>();
    Arrays.stream(material.columns())
        .forEach(
            name -> {
              if (name.equals("evidenceForOccurrenceID")) {
                selected.add(contextOccurrenceId.as(name));
              } else {
                selected.add(col("m." + name).as(name));
              }
            });

    return ctx.join(
            m,
            col("ctx." + SparkEventOccurrenceDiscovery.COL_MATERIAL_PK)
                .equalTo(col("m.materialEntity_pk")),
            "inner")
        .select(selected.toArray(Column[]::new));
  }

  private static Column contextOccurrenceId(String alias) {
    return concat(
        lit(CONTEXT_OCCURRENCE_PREFIX),
        col(alias + "." + SparkEventOccurrenceDiscovery.COL_EVENT_PK),
        lit(":"),
        col(alias + "." + SparkEventOccurrenceDiscovery.COL_OCCURRENCE_PK));
  }
}
