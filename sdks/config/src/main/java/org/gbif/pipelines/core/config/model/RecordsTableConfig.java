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

  /** When false, records are not written to HBase during indexing */
  private boolean enabled = false;

  /** HBase table name, e.g. "occurrence" */
  private String occurrenceTable;

  /**
   * Directory holding one manifest per dataset with the keys loaded into HBase, used to delete
   * records removed from a dataset. Must be outside the dataset/attempt directories so attempt
   * cleanup doesn't remove it.
   */
  private String manifestPath;

  /** Directory name (relative to the dataset/attempt output) used to stage HFiles */
  private String hfilePath = "records-hfile";

  /** Number of Delete mutations sent per batch when removing records */
  private int deleteBatchSize = 1_000;
}
