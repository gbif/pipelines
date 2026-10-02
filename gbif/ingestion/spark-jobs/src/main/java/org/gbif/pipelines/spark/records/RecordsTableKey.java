package org.gbif.pipelines.spark.records;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.keygen.Keygen;

/**
 * Row keys of the HBase records table: {@code <gbifId % 100, 2 digits>:<gbifId>}, e.g.
 * "67:1234567". The salt spreads sequential keys across regions and is derived from the key itself,
 * so a record is fetched with a single Get.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RecordsTableKey {

  public static final String COLUMN_FAMILY = "o";
  public static final String INTERPRETED_COLUMN = "interpreted";
  public static final String VERBATIM_COLUMN = "verbatim";
  public static final String DATASET_KEY_COLUMN = "datasetKey";
  public static final String ATTEMPT_COLUMN = "attempt";

  public static final int SALT_BUCKETS = 100;

  public static String rowKey(long gbifId) {
    return Keygen.getSaltedKey(gbifId);
  }

  public static byte[] rowKeyBytes(long gbifId) {
    return Bytes.toBytes(rowKey(gbifId));
  }

  /** Salt bucket of a row key, used to align Spark partitions with HBase regions */
  public static int salt(String rowKey) {
    return Integer.parseInt(rowKey.substring(0, rowKey.indexOf(':')));
  }
}
