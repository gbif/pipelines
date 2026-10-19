package org.gbif.pipelines.spark.util;

import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.FragmenterPipeline;

/** Helpers for reading the HBase fragment table populated by {@link FragmenterPipeline}. */
public final class FragmentTableUtil {

  /** Attempt used for fragment rows whose attempt is missing or can't be decoded. */
  public static final String UNKNOWN_ATTEMPT = "UNKNOWN";

  public static final byte[] FAMILY = Bytes.toBytes(FragmenterPipeline.FRAGMENT_COLUMN_FAMILY);
  public static final byte[] DATASET_KEY =
      Bytes.toBytes(FragmenterPipeline.FRAGMENT_DATASET_KEY_COLUMN);
  public static final byte[] ATTEMPT = Bytes.toBytes(FragmenterPipeline.FRAGMENT_ATTEMPT_COLUMN);

  private FragmentTableUtil() {}

  /** HBase configuration from the pipelines config, as used by the fragmenter. */
  public static Configuration hbaseConfiguration(PipelinesConfig config) {
    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.addResource(new Path(config.getHbaseSiteConfig()));
    if (config.getHdfsSiteConfig() != null && config.getCoreSiteConfig() != null) {
      hbaseConf.addResource(new Path(config.getHdfsSiteConfig()));
      hbaseConf.addResource(new Path(config.getCoreSiteConfig()));
    }
    return hbaseConf;
  }

  /**
   * Configuration for a {@link TableInputFormat} scan of the fragment table reading only the
   * datasetKey and attempt columns (never the large record column). Rows with neither column are
   * not returned by such a scan.
   */
  public static Configuration scanConfiguration(PipelinesConfig config) {
    Configuration hbaseConf = hbaseConfiguration(config);
    String family = FragmenterPipeline.FRAGMENT_COLUMN_FAMILY;
    hbaseConf.set(TableInputFormat.INPUT_TABLE, config.getFragmentsTable());
    hbaseConf.set(
        TableInputFormat.SCAN_COLUMNS,
        family
            + ":"
            + FragmenterPipeline.FRAGMENT_DATASET_KEY_COLUMN
            + " "
            + family
            + ":"
            + FragmenterPipeline.FRAGMENT_ATTEMPT_COLUMN);
    hbaseConf.set(TableInputFormat.SCAN_CACHEDROWS, "10000");
    hbaseConf.set(TableInputFormat.SCAN_CACHEBLOCKS, "false");
    return hbaseConf;
  }

  /** The fragment:datasetKey value, or null if the row doesn't have one. */
  public static String datasetKey(Result result) {
    byte[] value = result.getValue(FAMILY, DATASET_KEY);
    return value == null || value.length == 0 ? null : Bytes.toString(value);
  }

  /** The gbifId part of a fragment row key (salt:gbifId). */
  public static String gbifId(String rowKey) {
    return rowKey.substring(rowKey.indexOf(':') + 1);
  }

  /**
   * Decodes a fragment attempt cell. The current fragmenter writes it as text (e.g. "12"); older
   * rows hold a 4-byte big-endian int (e.g. \x00\x00\x00\xB0 = 176).
   */
  public static String decodeAttempt(byte[] value) {
    if (value == null || value.length == 0) {
      return UNKNOWN_ATTEMPT;
    }
    boolean digits = true;
    for (byte b : value) {
      if (b < '0' || b > '9') {
        digits = false;
        break;
      }
    }
    if (digits) {
      return new String(value, StandardCharsets.US_ASCII);
    }
    if (value.length == Integer.BYTES) {
      return String.valueOf(Bytes.toInt(value));
    }
    return UNKNOWN_ATTEMPT;
  }
}
