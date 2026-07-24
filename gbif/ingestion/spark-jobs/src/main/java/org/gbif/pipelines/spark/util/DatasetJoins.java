package org.gbif.pipelines.spark.util;

import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Small Spark {@link Dataset} join helpers shared across the DwC-DP builder packages.
 */
public final class DatasetJoins {

  private DatasetJoins() {}

  /**
   * Left-outer-joins {@code rightOpt} onto {@code left} via {@code joinColumn} when present,
   * dropping the duplicate join column from the right side afterwards. Returns {@code left}
   * unchanged when {@code rightOpt} is {@link Optional#empty()} — the standard shape for an
   * optional extension/enrichment Dataset that may not exist in a given DwC-DP package.
   *
   * @param left the Dataset to enrich
   * @param rightOpt the optional Dataset to join on, e.g. the result of an extension builder
   * @param joinColumn column name present on both sides to join on
   */
  public static Dataset<Row> leftJoinIfPresent(
    Dataset<Row> left, Optional<Dataset<Row>> rightOpt, String joinColumn) {
    return rightOpt
      .map(
        right ->
          left.join(right, left.col(joinColumn).equalTo(right.col(joinColumn)), "left_outer")
            .drop(right.col(joinColumn)))
      .orElse(left);
  }
}
