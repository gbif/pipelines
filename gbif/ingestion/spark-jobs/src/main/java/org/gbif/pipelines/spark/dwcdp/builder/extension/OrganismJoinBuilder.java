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
 * Enriches occurrence rows with fields from {@code organism} (no DwC-A Organism extension exists —
 * fields must be denormalized directly onto the core row).
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>occurrence.organismID = organism.organismID (left outer, direct FK)
 * </ul>
 *
 * <p>Occurrence's own value always wins on overlapping fields (e.g. {@code organismName}); only
 * columns organism has that occurrence doesn't (notably {@code associatedOrganisms}) are added.
 */
@Slf4j
public class OrganismJoinBuilder {

  public static final String TABLE_ORGANISM = "organism";
  static final String JOIN_KEY = "organismID";

  private OrganismJoinBuilder() {}

  /** {@code occurrenceDf} unchanged if organism is absent, or occurrence lacks {@code organismID}. */
  public static Dataset<Row> enrichOccurrences(TableLoader loader, Dataset<Row> occurrenceDf) {
    Optional<Dataset<Row>> organismDf = loader.load(TABLE_ORGANISM);
    if (organismDf.isEmpty()) {
      log.debug("No organism table present; skipping organism join");
      return occurrenceDf;
    }

    if (!Arrays.asList(occurrenceDf.columns()).contains(JOIN_KEY)) {
      log.warn("occurrence table has no {} column; skipping organism join", JOIN_KEY);
      return occurrenceDf;
    }

    return joinOrganism(occurrenceDf, organismDf.get());
  }

  /** Pure join transform, separated from I/O for direct unit testing. */
  static Dataset<Row> joinOrganism(Dataset<Row> occurrenceDf, Dataset<Row> organismDf) {
    Set<String> occurrenceCols = new HashSet<>(Arrays.asList(occurrenceDf.columns()));

    List<Column> selectCols = new ArrayList<>();
    for (String col : occurrenceDf.columns()) {
      selectCols.add(occurrenceDf.col(col));
    }
    for (String col : organismDf.columns()) {
      if (!occurrenceCols.contains(col) && !col.equals(JOIN_KEY)) {
        selectCols.add(organismDf.col(col));
        log.debug("Adding organism column '{}' to occurrence rows", col);
      }
    }

    Dataset<Row> joined =
        occurrenceDf
            .join(
                organismDf,
                occurrenceDf.col(JOIN_KEY).equalTo(organismDf.col(JOIN_KEY)),
                "left_outer")
            .select(selectCols.toArray(new Column[0]));

    log.info(
        "Organism join complete: occurrence columns before={}, after={}",
        occurrenceDf.columns().length,
        joined.columns().length);

    return joined;
  }

  /** Same three-bucket shape as {@link GeologicalContextJoinBuilder#computeFunnel}. */
  public static Optional<JoinFunnel> computeFunnel(TableLoader loader) {
    Optional<Dataset<Row>> occurrenceDfOpt = loader.load("occurrence");
    if (occurrenceDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> occurrenceDf = occurrenceDfOpt.get();
    if (!Arrays.asList(occurrenceDf.columns()).contains(JOIN_KEY)) {
      return Optional.empty();
    }

    String label = "OrganismJoinBuilder (occurrence." + JOIN_KEY + ")";
    long candidates = occurrenceDf.filter(functions.col(JOIN_KEY).isNotNull()).count();
    if (candidates == 0L) {
      return Optional.of(new JoinFunnel(label, List.of(new JoinFunnel.Bucket("candidates", 0L))));
    }

    Optional<Dataset<Row>> organismDfOpt = loader.load(TABLE_ORGANISM);
    if (organismDfOpt.isEmpty()) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(
                  new JoinFunnel.Bucket("candidates (" + JOIN_KEY + " set)", candidates),
                  new JoinFunnel.Bucket("organism table absent, unresolved", candidates))));
    }

    Dataset<Row> organismIds =
        organismDfOpt.get().select(functions.col(JOIN_KEY).as("__organism_funnel_key")).distinct();
    long resolved =
        occurrenceDf
            .filter(functions.col(JOIN_KEY).isNotNull())
            .join(
                organismIds,
                occurrenceDf.col(JOIN_KEY).equalTo(organismIds.col("__organism_funnel_key")),
                "left_semi")
            .count();
    long unresolved = candidates - resolved;

    return Optional.of(
        new JoinFunnel(
            label,
            List.of(
                new JoinFunnel.Bucket("candidates (" + JOIN_KEY + " set)", candidates),
                new JoinFunnel.Bucket("resolved", resolved),
                new JoinFunnel.Bucket("no matching organismID, unresolved", unresolved))));
  }
}
