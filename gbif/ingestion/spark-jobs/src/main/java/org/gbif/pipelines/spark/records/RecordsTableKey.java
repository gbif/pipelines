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
 *
 * <p>The records are in the {@value #DATA_FAMILY} family, read by the API with Gets. The columns
 * describing the load are in the small {@value #METADATA_FAMILY} family, stored in their own files,
 * so reports and checks by dataset scan it without reading the records.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RecordsTableKey {

  public static final String DATA_FAMILY = "d";
  public static final String METADATA_FAMILY = "m";

  // d
  public static final String INTERPRETED_COLUMN = "interpreted";
  public static final String VERBATIM_COLUMN = "verbatim";

  // m
  public static final String DATASET_KEY_COLUMN = "datasetKey";
  public static final String ATTEMPT_COLUMN = "attempt";

  /** Family of a column */
  public static String family(String column) {
    return switch (column) {
      case INTERPRETED_COLUMN, VERBATIM_COLUMN -> DATA_FAMILY;
      case DATASET_KEY_COLUMN, ATTEMPT_COLUMN -> METADATA_FAMILY;
      default -> throw new IllegalArgumentException("Unknown column " + column);
    };
  }

  public static String occurrenceRowKey(long gbifId) {
    return Keygen.getSaltedKey(gbifId);
  }

  public static String eventRowKey(String internalId) {
    return internalId;
  }
}
