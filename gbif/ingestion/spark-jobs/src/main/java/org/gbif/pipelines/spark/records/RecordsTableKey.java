package org.gbif.pipelines.spark.records;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.keygen.Keygen;

/**
 * Row keys and columns of the HBase records tables.
 *
 * <ul>
 *   <li>Occurrences: {@code <gbifId % 100, 2 digits>:<gbifId>}, e.g. "67:1234567". The salt spreads
 *       sequential keys across regions and is derived from the key itself, so a record is fetched
 *       with a single Get.
 *   <li>Events: the internalId, a SHA-1 hex string that is already evenly distributed.
 * </ul>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RecordsTableKey {

  public static final String COLUMN_FAMILY = "o";
  public static final String INTERPRETED_COLUMN = "interpreted";
  public static final String VERBATIM_COLUMN = "verbatim";
  public static final String DATASET_KEY_COLUMN = "datasetKey";
  public static final String ATTEMPT_COLUMN = "attempt";

  public static String occurrenceRowKey(long gbifId) {
    return Keygen.getSaltedKey(gbifId);
  }

  public static String eventRowKey(String internalId) {
    return internalId;
  }
}
