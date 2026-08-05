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
 * Enriches rows by left-joining the {@code usage-policy} table onto them.
 *
 * <p>Like {@link OrganismJoinBuilder} and {@link GeologicalContextJoinBuilder}, this denormalizes a
 * related table's fields directly onto the row rather than producing a separate extension row type
 * — {@code license} and {@code rightsHolder} (the two fields {@link
 * org.gbif.pipelines.core.interpreters.extension.MultimediaInterpreter} reads via {@code
 * DcTerm.license}/{@code DcTerm.rightsHolder}) exist only on {@code usage-policy}, never on the
 * entity's own table — {@code media} and {@code material} both only carry the surrogate {@code
 * usagePolicy_fk}.
 *
 * <p>Both {@code license} and {@code rightsHolder} match their target DwC-A terms' simple names
 * directly, so once joined they resolve correctly via {@code TermFactory} with no rename needed —
 * only the join itself was missing.
 *
 * <p>Entity-agnostic: nothing in {@link #enrich}/{@link #join} is specific to {@code media} —
 * originally written for it (hence the historical method name below), now used identically for
 * {@code material} too, rather than duplicating the same join logic under a second name.
 */
@Slf4j
public class UsagePolicyJoinBuilder {

  public static final String TABLE_USAGE_POLICY = "usage-policy";
  static final String USAGE_POLICY_FK_COLUMN = "usagePolicy_fk";
  static final String USAGE_POLICY_PK_COLUMN = "usagePolicy_pk";

  private UsagePolicyJoinBuilder() {}

  /**
   * Returns {@code df} enriched with usage-policy columns (notably {@code license}, {@code
   * rightsHolder}), or the original {@code df} unchanged if the usage-policy table is absent or
   * {@code df} carries no {@code usagePolicy_fk} column.
   *
   * @param loader table loader — returns {@link Optional#empty()} when usage-policy is absent
   * @param df the Dataset to enrich (e.g. {@code media} or {@code material})
   * @return rows with additional usage-policy fields merged in
   */
  public static Dataset<Row> enrich(TableLoader loader, Dataset<Row> df) {
    Optional<Dataset<Row>> usagePolicyDf = loader.load(TABLE_USAGE_POLICY);
    if (usagePolicyDf.isEmpty()) {
      log.debug("No usage-policy table present; skipping usage policy join");
      return df;
    }

    if (!Arrays.asList(df.columns()).contains(USAGE_POLICY_FK_COLUMN)) {
      log.debug("no {} column present; skipping usage policy join", USAGE_POLICY_FK_COLUMN);
      return df;
    }

    return join(df, usagePolicyDf.get());
  }

  /**
   * Pure join transform — separated from I/O so it can be unit tested directly with in-memory
   * Datasets, same shape as {@link OrganismJoinBuilder#joinOrganism} and {@link
   * GeologicalContextJoinBuilder#join}.
   *
   * <p>Columns already present on {@code df} are never overwritten by usage-policy columns. Both
   * the join key ({@code usagePolicy_fk} on {@code df}'s side, resolved against {@code
   * usagePolicy_pk}) and usage-policy's own surrogate PK are dropped afterwards.
   */
  static Dataset<Row> join(Dataset<Row> df, Dataset<Row> usagePolicyDf) {
    Set<String> existingCols = new HashSet<>(Arrays.asList(df.columns()));

    List<Column> selectCols = new ArrayList<>();
    for (String col : df.columns()) {
      selectCols.add(df.col(col));
    }
    for (String col : usagePolicyDf.columns()) {
      if (!existingCols.contains(col) && !col.equals(USAGE_POLICY_PK_COLUMN)) {
        selectCols.add(usagePolicyDf.col(col));
        log.debug("Adding usage-policy column '{}' to rows", col);
      }
    }

    Dataset<Row> joined =
        df.join(
                usagePolicyDf,
                df.col(USAGE_POLICY_FK_COLUMN).equalTo(usagePolicyDf.col(USAGE_POLICY_PK_COLUMN)),
                "left_outer")
            .select(selectCols.toArray(new Column[0]))
            .drop(USAGE_POLICY_FK_COLUMN);

    log.info(
        "Usage policy join complete: columns before={}, after={}",
        df.columns().length,
        joined.columns().length);

    return joined;
  }

  /**
   * Computes a {@link JoinFunnel} breakdown of {@code usagePolicy_fk} resolution for one entity
   * table (e.g. {@code "media"} or {@code "material"} — {@link #enrich} is entity-agnostic, so this
   * is too), mirroring {@link #enrich}'s decision logic. Buckets are mutually exclusive and sum to
   * the candidate count.
   *
   * @param entityTable the table {@code usagePolicy_fk} lives on, e.g. {@code "media"} or {@code
   *     "material"}
   * @return empty if {@code entityTable} is absent, or present but missing {@code usagePolicy_fk}
   *     entirely
   */
  public static Optional<JoinFunnel> computeFunnel(TableLoader loader, String entityTable) {
    Optional<Dataset<Row>> entityDfOpt = loader.load(entityTable);
    if (entityDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> entityDf = entityDfOpt.get();
    if (!Arrays.asList(entityDf.columns()).contains(USAGE_POLICY_FK_COLUMN)) {
      return Optional.empty();
    }

    String label = "UsagePolicyJoinBuilder (" + entityTable + "." + USAGE_POLICY_FK_COLUMN + ")";
    long candidates = entityDf.filter(functions.col(USAGE_POLICY_FK_COLUMN).isNotNull()).count();
    if (candidates == 0L) {
      return Optional.of(new JoinFunnel(label, List.of(new JoinFunnel.Bucket("candidates", 0L))));
    }

    Optional<Dataset<Row>> usagePolicyDfOpt = loader.load(TABLE_USAGE_POLICY);
    if (usagePolicyDfOpt.isEmpty()) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(
                  new JoinFunnel.Bucket(
                      "candidates (" + USAGE_POLICY_FK_COLUMN + " set)", candidates),
                  new JoinFunnel.Bucket("usage-policy table absent, unresolved", candidates))));
    }

    Dataset<Row> usagePolicyIds =
        usagePolicyDfOpt
            .get()
            .select(functions.col(USAGE_POLICY_PK_COLUMN).as("__usage_policy_funnel_key"))
            .distinct();
    long resolved =
        entityDf
            .filter(functions.col(USAGE_POLICY_FK_COLUMN).isNotNull())
            .join(
                usagePolicyIds,
                entityDf
                    .col(USAGE_POLICY_FK_COLUMN)
                    .equalTo(usagePolicyIds.col("__usage_policy_funnel_key")),
                "left_semi")
            .count();
    long unresolved = candidates - resolved;

    return Optional.of(
        new JoinFunnel(
            label,
            List.of(
                new JoinFunnel.Bucket(
                    "candidates (" + USAGE_POLICY_FK_COLUMN + " set)", candidates),
                new JoinFunnel.Bucket("resolved", resolved),
                new JoinFunnel.Bucket(
                    "dangling FK, no matching usagePolicy_pk (value dropped)", unresolved))));
  }
}
