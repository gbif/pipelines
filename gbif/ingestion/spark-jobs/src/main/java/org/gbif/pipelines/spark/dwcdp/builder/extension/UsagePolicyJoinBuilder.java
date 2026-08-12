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
 * Enriches rows with {@code license}/{@code rightsHolder} from {@code usage-policy}. Entity-agnostic
 * — used for both {@code media} and {@code material}.
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>usagePolicy_fk = usage-policy.usagePolicy_pk (left outer)
 * </ul>
 */
@Slf4j
public class UsagePolicyJoinBuilder {

  public static final String TABLE_USAGE_POLICY = "usage-policy";
  static final String USAGE_POLICY_FK_COLUMN = "usagePolicy_fk";
  static final String USAGE_POLICY_PK_COLUMN = "usagePolicy_pk";

  private UsagePolicyJoinBuilder() {}

  /** {@code df} unchanged if usage-policy is absent, or {@code df} lacks {@code usagePolicy_fk}. */
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

  /** Pure join transform, separated from I/O for direct unit testing. */
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

  /** Entity-agnostic, same as {@link #enrich}. */
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
