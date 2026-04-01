package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableBuildConfig {
  String write_format_default = "parquet";
  String parquet_compression = "ZSTD";
  String auto_purge = "true";
  String write_merge_isolation_level = "snapshot";
  String commit_retry_num_retries = "10";
  String commit_retry_min_wait_ms = "1000";
  String commit_retry_max_wait_ms = "10000";
  String history_expire_max_snapshot_age_ms = "3888000000";
  String history_expire_min_snapshots_to_keep = "10";
}
