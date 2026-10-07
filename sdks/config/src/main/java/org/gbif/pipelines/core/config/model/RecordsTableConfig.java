package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration of the HBase table holding the API representation of occurrence records, used to
 * serve record details (occurrence/{key} and occurrence/{key}/verbatim) without reading
 * Elasticsearch documents.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordsTableConfig implements Serializable {

  /** HBase table holding occurrence records, keyed by salted gbifId */
  private String occurrenceTable;

  /** HBase table holding event records, keyed by internalId */
  private String eventTable;

  /**
   * Directory holding one manifest per dataset with the keys loaded into HBase, used to delete
   * records removed from a dataset. Must be outside the dataset/attempt directories so attempt
   * cleanup doesn't remove it.
   */
  private String manifestPath;

  /** Directory name (relative to the dataset/attempt output) used to stage HFiles */
  private String hfilePath = "records-hfile";

  /**
   * Loads with more records are written as HFiles and bulk loaded, smaller ones with Puts, as the
   * HFiles of a small load add store files to the regions for little gain
   */
  private long bulkLoadIfRecordsMoreThan = 50_000;

  /** Number of Delete mutations sent per batch when removing records */
  private int deleteBatchSize = 1_000;
}
