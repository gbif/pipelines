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
 * Enriches event rows by left-joining the {@code geological-context} table onto them.
 *
 * <p><b>This is not a DwC-A extension builder</b> — like {@link OrganismJoinBuilder}, it
 * denormalizes a related table's fields directly onto the core record rather than producing a
 * separate extension row type. {@code
 * org.gbif.pipelines.core.interpreters.core.GeologicalContextInterpreter} on the DwC-A ingestion
 * side reads chronostratigraphy fields ({@code earliestEonOrLowestEonothem}, {@code
 * lowestBiostratigraphicZone}, {@code formation}, etc.) directly off the core record's term map —
 * there is no GeologicalContext extension for those fields to live in instead.
 *
 * <p>In DwC-DP those fields live on a standalone {@code geological-context} table, linked from
 * {@code event} via {@code event.geologicalContextID} — a <em>weak</em> FK to {@code
 * geological-context.geologicalContextID}. Both sides are natural identifiers (unlike most other
 * joins in this codebase), so no surrogate {@code _pk}/{@code _fk} resolution step is needed here,
 * just a direct join on the shared natural key.
 *
 * <p>Material-linked geological context is handled separately by {@link
 * MaterialGeologicalContextJoinBuilder}, because its junction-table cardinality needs an explicit
 * unambiguous-link policy before fields can be flattened onto an occurrence.
 */
@Slf4j
public class GeologicalContextJoinBuilder {

  public static final String TABLE_GEOLOGICAL_CONTEXT = "geological-context";
  static final String JOIN_KEY = "geologicalContextID";

  private GeologicalContextJoinBuilder() {}

  /**
   * Returns {@code eventDf} enriched with geological-context columns, or the original {@code
   * eventDf} unchanged if the geological-context table is absent or the event table carries no
   * {@code geologicalContextID} column.
   *
   * @param loader table loader — returns {@link Optional#empty()} when the geological-context table
   *     is absent
   * @param eventDf the event Dataset to enrich
   * @return event rows with additional geological-context fields merged in
   */
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

  /**
   * Pure join transform — separated from I/O so it can be unit tested directly with in-memory
   * Datasets, same shape as {@link OrganismJoinBuilder#joinOrganism}.
   *
   * <p>Columns already present on {@code eventDf} are never overwritten by geological-context
   * columns (none currently overlap per the DwC-DP schema, but the guard is kept for the same
   * defensive consistency {@link OrganismJoinBuilder} applies). The join key itself and the
   * geological-context table's own surrogate PK ({@code geologicalContext_pk}) are excluded from
   * the geological-context side of the select — {@code event}'s own {@code geologicalContextID} is
   * preserved, and the surrogate PK has no business appearing in an event's core terms.
   */
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

  /**
   * Computes a {@link JoinFunnel} breakdown of {@code event.geologicalContextID} resolution,
   * mirroring {@link #enrichEvents}'s decision logic. Buckets are mutually exclusive and sum to the
   * candidate count:
   *
   * <ul>
   *   <li><b>geological-context table absent, unresolved</b> — {@link #enrichEvents} is a no-op for
   *       every candidate row in this case
   *   <li><b>resolved</b> — {@code geologicalContextID} matched a row in {@code geological-context}
   *   <li><b>no matching geologicalContextID, unresolved</b> — populated but no match; {@code
   *       geologicalContextID} itself is a natural key shared on both sides, so this is a dangling
   *       reference rather than an FK-resolution gap
   * </ul>
   *
   * @return empty if {@code event} is absent, or present but missing {@code geologicalContextID}
   *     entirely
   */
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
