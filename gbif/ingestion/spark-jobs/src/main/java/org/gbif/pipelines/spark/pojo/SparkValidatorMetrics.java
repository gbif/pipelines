package org.gbif.pipelines.spark.pojo;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.validator.api.Metrics.IssueInfo;

/** Metrics computed by {@link org.gbif.pipelines.spark.ValidatorMetricsPipeline}. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkValidatorMetrics {

  /** Total number of indexed (interpreted) occurrence records. */
  private long indexedCount;

  /**
   * Per-issue aggregation: count of records affected by each issue and up to five sample records
   * showing verbatim values for related terms.
   */
  private List<IssueInfo> issues;

  /**
   * Count of records with a non-null interpreted value for each term. Keys are ES field paths as
   * returned by {@link org.gbif.pipelines.validator.metrics.RawToInterpreted#getInterpretedField},
   * e.g. {@code "decimalLatitude"} or {@code "gbifClassification.kingdom"}.
   */
  private Map<String, Long> interpretedFieldCounts;
}
