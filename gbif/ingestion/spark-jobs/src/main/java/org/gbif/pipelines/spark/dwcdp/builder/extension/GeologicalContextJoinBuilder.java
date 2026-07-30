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
}
