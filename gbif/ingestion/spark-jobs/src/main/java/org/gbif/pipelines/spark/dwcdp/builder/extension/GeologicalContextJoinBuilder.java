package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Enriches event rows with chronostratigraphy fields from {@code geological-context} (read
 * directly off the core term map — DwC-A has no GeologicalContext extension).
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>event.geologicalContextID = geological-context.geologicalContextID (left outer,
 *       natural-key, weak FK)
 * </ul>
 *
 * <p>Material-linked geological context is a separate case — see {@link
 * MaterialGeologicalContextJoinBuilder}.
 */
@Slf4j
public class GeologicalContextJoinBuilder {

  public static final String TABLE_GEOLOGICAL_CONTEXT = "geological-context";
  static final String JOIN_KEY = "geologicalContextID";

  private GeologicalContextJoinBuilder() {}

  /** {@code eventDf} unchanged if geological-context is absent, or event lacks {@code geologicalContextID}. */
  public static Dataset<Row> enrichEvents(TableLoader loader, Dataset<Row> eventDf) {
    Optional<Dataset<Row>> geoDf = loader.load(TABLE_GEOLOGICAL_CONTEXT);
    if (geoDf.isEmpty()) {
      log.debug("No geological-context table present; skipping geological context join");
      return eventDf;
    }

    if (!Arrays.asList(eventDf.columns()).contains(JOIN_KEY)) {
      log.warn("event table has no {} column; skipping geological context join", JOIN_KEY);
      return eventDf;
    }

    return join(eventDf, geoDf.get());
  }

  /** Pure join transform, separated from I/O for direct unit testing. */
  static Dataset<Row> join(Dataset<Row> eventDf, Dataset<Row> geoDf) {
    Set<String> eventCols = new HashSet<>(Arrays.asList(eventDf.columns()));

    List<Column> selectCols = new ArrayList<>();
    for (String col : eventDf.columns()) {
      selectCols.add(eventDf.col(col));
    }
    for (String col : geoDf.columns()) {
      if (!eventCols.contains(col)
          && !col.equals(JOIN_KEY)
          && !col.equals("geologicalContext_pk")) {
        selectCols.add(geoDf.col(col));
        log.debug("Adding geological-context column '{}' to event rows", col);
      }
    }

    Dataset<Row> joined =
        eventDf
            .join(geoDf, eventDf.col(JOIN_KEY).equalTo(geoDf.col(JOIN_KEY)), "left_outer")
            .select(selectCols.toArray(new Column[0]));

    log.info(
        "Geological context join complete: event columns before={}, after={}",
        eventDf.columns().length,
        joined.columns().length);

    return joined;
  }

  /** Buckets: table absent (unresolved) / resolved / dangling reference (unresolved). */
  public static Optional<JoinFunnel> computeFunnel(TableLoader loader) {
    Optional<Dataset<Row>> eventDfOpt = loader.load("event");
    if (eventDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> eventDf = eventDfOpt.get();
    if (!Arrays.asList(eventDf.columns()).contains(JOIN_KEY)) {
      return Optional.empty();
    }

    String label = "GeologicalContextJoinBuilder (event." + JOIN_KEY + ")";
    long candidates = eventDf.filter(functions.col(JOIN_KEY).isNotNull()).count();
    if (candidates == 0L) {
      return Optional.of(new JoinFunnel(label, List.of(new JoinFunnel.Bucket("candidates", 0L))));
    }

    Optional<Dataset<Row>> geoDfOpt = loader.load(TABLE_GEOLOGICAL_CONTEXT);
    if (geoDfOpt.isEmpty()) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(
                  new JoinFunnel.Bucket("candidates (" + JOIN_KEY + " set)", candidates),
                  new JoinFunnel.Bucket(
                      "geological-context table absent, unresolved", candidates))));
    }

    Dataset<Row> geoIds =
        geoDfOpt.get().select(functions.col(JOIN_KEY).as("__geo_context_funnel_key")).distinct();
    long resolved =
        eventDf
            .filter(functions.col(JOIN_KEY).isNotNull())
            .join(
                geoIds,
                eventDf.col(JOIN_KEY).equalTo(geoIds.col("__geo_context_funnel_key")),
                "left_semi")
            .count();
    long unresolved = candidates - resolved;

    return Optional.of(
        new JoinFunnel(
            label,
            List.of(
                new JoinFunnel.Bucket("candidates (" + JOIN_KEY + " set)", candidates),
                new JoinFunnel.Bucket("resolved", resolved),
                new JoinFunnel.Bucket("no matching geologicalContextID, unresolved", unresolved))));
  }
}
