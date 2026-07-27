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
 * Enriches occurrence rows by left-joining the {@code organism} table onto them.
 *
 * <p><b>This is not a DwC-A extension builder.</b> DwC-A has no Organism extension; organism fields
 * must be denormalized directly onto each Occurrence core row. This join produces that
 * denormalization in a distributed Spark step.
 *
 * <p>The join key is {@code organismID}, which the DwC-DP occurrence schema carries as a direct FK
 * to the organism table's primary key. The result is a many:1 collapse: if 5 occurrence rows
 * reference the same organism, all 5 will carry the same organism field values after the join.
 * Uniqueness of {@code organism.organismID} is guaranteed by the upstream DwC-DP validation step,
 * which rejects datasets where the primary key constraint is violated — so no defensive dedup is
 * needed here.
 *
 * <p><b>Column precedence policy:</b> the DwC-DP occurrence schema declares several organism fields
 * ({@code organismID}, {@code organismScope}, {@code organismName}, {@code causeOfDeath}, {@code
 * organismRemarks}) as denormalized columns directly on the occurrence table. When both occurrence
 * and organism carry a value for the same field (e.g. a publisher populates both {@code
 * occurrence.organismName} and {@code organism.organismName}), the <em>occurrence value silently
 * wins</em> — the organism column is excluded from the select because it is already present on
 * occurrence. Only columns present in the organism table but absent from the occurrence table (most
 * notably {@code associatedOrganisms}) are added. This policy is intentional: the occurrence row is
 * the authoritative source for occurrence-level organism attributes.
 */
@Slf4j
public class OrganismJoinBuilder {

  public static final String TABLE_ORGANISM = "organism";
  static final String JOIN_KEY = "organismID";

  private OrganismJoinBuilder() {}

  /**
   * Returns {@code occurrenceDf} enriched with organism columns, or the original {@code
   * occurrenceDf} unchanged if the organism table is absent or the occurrence table carries no
   * {@code organismID} column.
   *
   * @param loader table loader — returns {@link Optional#empty()} when organism table is absent
   * @param occurrenceDf the occurrence Dataset to enrich
   * @return occurrence rows with additional organism fields merged in
   */
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

  /**
   * Pure join transform — separated from I/O so it can be unit tested directly with in-memory
   * Datasets without any filesystem involvement.
   *
   * <p>Columns already present on {@code occurrenceDf} are never overwritten by organism columns —
   * see class-level Javadoc for the precedence policy. The join key {@code organismID} is also
   * excluded from the organism side of the select to avoid ambiguity; the occurrence's own {@code
   * organismID} column is preserved and will appear in {@code coreTerms} so downstream
   * interpretation can follow the FK if needed.
   */
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
}
