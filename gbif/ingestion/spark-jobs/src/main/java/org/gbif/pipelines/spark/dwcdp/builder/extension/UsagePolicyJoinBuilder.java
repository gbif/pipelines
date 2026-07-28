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
 * Enriches {@code media} rows by left-joining the {@code usage-policy} table onto them.
 *
 * <p>Like {@link OrganismJoinBuilder} and {@link GeologicalContextJoinBuilder}, this denormalizes a
 * related table's fields directly onto the row rather than producing a separate extension row type
 * — {@code license} and {@code rightsHolder} (the two fields {@link
 * org.gbif.pipelines.core.interpreters.extension.MultimediaInterpreter} reads via {@code
 * DcTerm.license}/{@code DcTerm.rightsHolder}) exist only on {@code usage-policy}, never on {@code
 * media} itself — {@code media} only carries the surrogate {@code usagePolicy_fk}.
 *
 * <p>Both {@code license} and {@code rightsHolder} match their target DwC-A terms' simple names
 * directly, so once joined they resolve correctly via {@code TermFactory} with no rename needed —
 * only the join itself was missing.
 *
 * <p>Only {@code media} is handled here. {@code material} also carries a {@code usagePolicy_fk} per
 * the DwC-DP schema, but {@code material} itself isn't joined anywhere in this pipeline yet.
 */
@Slf4j
public class UsagePolicyJoinBuilder {

  public static final String TABLE_USAGE_POLICY = "usage-policy";
  static final String USAGE_POLICY_FK_COLUMN = "usagePolicy_fk";
  static final String USAGE_POLICY_PK_COLUMN = "usagePolicy_pk";

  private UsagePolicyJoinBuilder() {}

  /**
   * Returns {@code mediaDf} enriched with usage-policy columns (notably {@code license}, {@code
   * rightsHolder}), or the original {@code mediaDf} unchanged if the usage-policy table is absent
   * or {@code media} carries no {@code usagePolicy_fk} column.
   *
   * @param loader table loader — returns {@link Optional#empty()} when usage-policy is absent
   * @param mediaDf the media Dataset to enrich
   * @return media rows with additional usage-policy fields merged in
   */
  public static Dataset<Row> enrichMedia(TableLoader loader, Dataset<Row> mediaDf) {
    Optional<Dataset<Row>> usagePolicyDf = loader.load(TABLE_USAGE_POLICY);
    if (usagePolicyDf.isEmpty()) {
      log.debug("No usage-policy table present; skipping usage policy join");
      return mediaDf;
    }

    if (!Arrays.asList(mediaDf.columns()).contains(USAGE_POLICY_FK_COLUMN)) {
      log.warn("media table has no {} column; skipping usage policy join", USAGE_POLICY_FK_COLUMN);
      return mediaDf;
    }

    return join(mediaDf, usagePolicyDf.get());
  }

  /**
   * Pure join transform — separated from I/O so it can be unit tested directly with in-memory
   * Datasets, same shape as {@link OrganismJoinBuilder#joinOrganism} and {@link
   * GeologicalContextJoinBuilder#join}.
   *
   * <p>Columns already present on {@code mediaDf} are never overwritten by usage-policy columns
   * (media's own {@code license}/{@code rightsHolder} columns don't exist per the current schema,
   * but the guard is kept for the same defensive consistency the other join builders apply). Both
   * the join key ({@code usagePolicy_fk} on the media side, resolved against {@code
   * usagePolicy_pk}) and usage-policy's own surrogate PK are dropped afterwards.
   */
  static Dataset<Row> join(Dataset<Row> mediaDf, Dataset<Row> usagePolicyDf) {
    Set<String> mediaCols = new HashSet<>(Arrays.asList(mediaDf.columns()));

    List<Column> selectCols = new ArrayList<>();
    for (String col : mediaDf.columns()) {
      selectCols.add(mediaDf.col(col));
    }
    for (String col : usagePolicyDf.columns()) {
      if (!mediaCols.contains(col) && !col.equals(USAGE_POLICY_PK_COLUMN)) {
        selectCols.add(usagePolicyDf.col(col));
        log.debug("Adding usage-policy column '{}' to media rows", col);
      }
    }

    Dataset<Row> joined =
        mediaDf
            .join(
                usagePolicyDf,
                mediaDf
                    .col(USAGE_POLICY_FK_COLUMN)
                    .equalTo(usagePolicyDf.col(USAGE_POLICY_PK_COLUMN)),
                "left_outer")
            .select(selectCols.toArray(new Column[0]))
            .drop(USAGE_POLICY_FK_COLUMN);

    log.info(
        "Usage policy join complete: media columns before={}, after={}",
        mediaDf.columns().length,
        joined.columns().length);

    return joined;
  }
}
