package org.gbif.pipelines.spark.diag;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;
import static org.gbif.pipelines.core.utils.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.hbase.Columns;
import org.gbif.pipelines.spark.pojo.LookupRow;
import org.gbif.pipelines.spark.util.PipelineArgs;

/**
 * Exports the HBase occurrence lookup table rows (triplet/occurrenceID -> gbifID) of a list of
 * datasets to parquet, one directory per dataset: {@code <outputPath>/<datasetKey>/}.
 *
 * <p>The lookup table is salted ({@code NN:datasetKey|uniqueString}), so the rows of a dataset are
 * spread over all salt buckets. Each dataset is read with one bounded prefix scan per salt bucket
 * (one Spark task each), so only the rows of the requested datasets are read rather than the whole
 * table.
 *
 * <p>Each dataset is scanned into {@code <outputPath>/_tmp/<datasetKey>}, then rewritten into
 * {@code <outputPath>/<datasetKey>} as files of about {@code --rowsPerFile} rows, partitioned and
 * sorted by gbifId so that all keys of a gbifId are in the same file. Spark writes {@code _SUCCESS}
 * when the dataset is complete, and a re-run skips datasets that have it (unless {@code
 * --overwrite=true}). Counts are written to {@code <outputPath>/<datasetKey>/_export.yml}.
 *
 * <p>Several datasets are exported concurrently ({@code --parallelDatasets}), each as its own Spark
 * job.
 */
@Slf4j
public class IdentifierExportPipeline {

  public static final String DEFAULT_OUTPUT_PATH = "/data/identifier-export";
  public static final String METRICS_FILENAME = "_export.yml";
  static final String TMP_DIR = "_tmp";
  static final String SUCCESS_MARKER = "_SUCCESS";

  static final String OK = "OK";
  static final String SKIPPED = "SKIPPED";
  static final String FAILED = "FAILED";

  private static final int NUMBER_OF_BUCKETS = HBaseLockingKeyService.NUMBER_OF_BUCKETS;
  private static final String SALT_FORMAT =
      "%0" + String.valueOf(NUMBER_OF_BUCKETS - 1).length() + "d";

  @Parameters(separators = "=")
  private static class Args extends PipelineArgs {

    @Parameter(names = "--datasetIds", description = "Comma separated dataset keys to export")
    private List<String> datasetIds = new ArrayList<>();

    @Parameter(
        names = "--datasetIdsFile",
        description =
            "File with one dataset key per line (blank lines and # comments ignored), "
                + "default filesystem unless a scheme like file:/// is given")
    private String datasetIdsFile;

    @Parameter(names = "--outputPath", description = "Export root directory")
    private String outputPath = DEFAULT_OUTPUT_PATH;

    @Parameter(names = "--parallelDatasets", description = "Datasets exported concurrently")
    private int parallelDatasets = 4;

    @Parameter(names = "--rowsPerFile", description = "Approximate rows per output parquet file")
    private long rowsPerFile = 5_000_000L;

    @Parameter(names = "--scanCaching", description = "Rows fetched per HBase scanner RPC")
    private int scanCaching = 1_000;

    @Parameter(
        names = "--overwrite",
        description = "Re-export datasets that already have a completed export",
        arity = 1)
    private boolean overwrite = false;
  }

  public static void main(String[] argsv) throws Exception {

    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    SparkSession spark = null;
    FileSystem fileSystem = null;
    Map<String, String> result;

    try {
      /* ############ standard init block ########## */
      spark = getSparkSession(args.master, args.appName, config, (builder, cfg) -> {});
      fileSystem = getFileSystem(spark, config);
      /* ############ standard init block - end ########## */

      Set<String> datasetKeys = readDatasetKeys(spark, args.datasetIds, args.datasetIdsFile);

      result =
          export(
              spark,
              fileSystem,
              config,
              datasetKeys,
              args.outputPath,
              args.parallelDatasets,
              args.rowsPerFile,
              args.scanCaching,
              args.overwrite);
    } finally {
      if (fileSystem != null) {
        fileSystem.close();
      }
      if (spark != null) {
        spark.stop();
        spark.close();
      }
    }

    long failed = result.values().stream().filter(FAILED::equals).count();
    if (failed > 0) {
      log.error("{} datasets failed to export", failed);
      System.exit(1);
    }
    System.exit(0);
  }

  /**
   * Exports the lookup rows of the given datasets.
   *
   * @return status (OK, SKIPPED or FAILED) per dataset key
   */
  public static Map<String, String> export(
      SparkSession spark,
      FileSystem fs,
      PipelinesConfig config,
      Collection<String> datasetKeys,
      String outputPath,
      int parallelDatasets,
      long rowsPerFile,
      int scanCaching,
      boolean overwrite)
      throws Exception {

    long start = System.currentTimeMillis();
    log.info(
        "Exporting {} datasets from {} into {}, {} at a time",
        datasetKeys.size(),
        config.getKeygen().getLookupTable(),
        outputPath,
        parallelDatasets);

    Map<String, Future<String>> futures = new LinkedHashMap<>();
    ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, parallelDatasets));
    try {
      for (String datasetKey : datasetKeys) {
        futures.put(
            datasetKey,
            executor.submit(
                () ->
                    exportDataset(
                        spark,
                        fs,
                        config,
                        datasetKey,
                        outputPath,
                        rowsPerFile,
                        scanCaching,
                        overwrite)));
      }
      executor.shutdown();

      Map<String, String> result = new LinkedHashMap<>();
      for (Map.Entry<String, Future<String>> e : futures.entrySet()) {
        result.put(e.getKey(), e.getValue().get());
      }

      Path tmp = new Path(outputPath, TMP_DIR);
      if (fs.exists(tmp) && fs.listStatus(tmp).length == 0) {
        fs.delete(tmp, true);
      }

      log.info(
          "Finished export in {} secs: {} OK, {} skipped, {} failed {}",
          (System.currentTimeMillis() - start) / 1000,
          result.values().stream().filter(OK::equals).count(),
          result.values().stream().filter(SKIPPED::equals).count(),
          result.values().stream().filter(FAILED::equals).count(),
          result);
      return result;
    } finally {
      executor.shutdownNow();
    }
  }

  private static String exportDataset(
      SparkSession spark,
      FileSystem fs,
      PipelinesConfig config,
      String datasetKey,
      String outputPath,
      long rowsPerFile,
      int scanCaching,
      boolean overwrite) {

    Path finalDir = new Path(outputPath, datasetKey);
    Path tmpDir = new Path(new Path(outputPath, TMP_DIR), datasetKey);
    long start = System.currentTimeMillis();

    try {
      if (!overwrite && fs.exists(new Path(finalDir, SUCCESS_MARKER))) {
        log.info("Skipping {}, {} is already complete", datasetKey, finalDir);
        return SKIPPED;
      }

      spark.sparkContext().setJobGroup(datasetKey, "Export identifiers " + datasetKey, false);

      // 1. scan - one task per salt bucket
      LookupScanFunction scanFn =
          new LookupScanFunction(
              config.getKeygen().getZkConnectionString(),
              config.getKeygen().getHbaseZnode(),
              config.getKeygen().getLookupTable(),
              datasetKey,
              scanCaching);

      spark
          .range(0, NUMBER_OF_BUCKETS, 1, NUMBER_OF_BUCKETS)
          .flatMap(scanFn, Encoders.bean(LookupRow.class))
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(tmpDir.toString());

      // 2. rewrite in sensibly sized files, grouped and sorted by gbifId
      Dataset<Row> scanned = spark.read().parquet(tmpDir.toString());
      long rows = scanned.count();
      int files = (int) Math.max(1L, (rows + rowsPerFile - 1) / rowsPerFile);

      scanned
          .repartition(files, col("gbifId"))
          .sortWithinPartitions("gbifId", "keyType", "uniqueKey")
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(finalDir.toString());

      // 3. counts
      Map<String, Long> metrics = metrics(scanned);
      writeMetricsYaml(fs, metrics, new Path(finalDir, METRICS_FILENAME).toString());

      fs.delete(tmpDir, true);

      log.info(
          "Exported dataset {} in {} secs: {}",
          datasetKey,
          (System.currentTimeMillis() - start) / 1000,
          metrics);
      return OK;

    } catch (Exception ex) {
      log.error("Export of dataset {} failed", datasetKey, ex);
      try {
        fs.delete(tmpDir, true);
      } catch (IOException e) {
        log.warn("Could not delete {}", tmpDir, e);
      }
      return FAILED;
    } finally {
      spark.sparkContext().clearJobGroup();
    }
  }

  static Map<String, Long> metrics(Dataset<Row> rows) {
    Row totals =
        rows.agg(
                count(lit(1)).as("rows"),
                countIf(col("keyType").equalTo(LookupRow.OCCURRENCE_ID)).as("occurrenceIds"),
                countIf(col("keyType").equalTo(LookupRow.TRIPLET)).as("triplets"),
                countIf(col("gbifId").isNull()).as("missingGbifId"),
                countIf(col("lock").isNotNull()).as("locked"),
                countDistinct("gbifId").as("distinctGbifIds"))
            .first();

    // a gbifID is expected to have at most one occurrenceID and one triplet
    long reused =
        rows.filter(col("gbifId").isNotNull())
            .groupBy("gbifId")
            .agg(
                countIf(col("keyType").equalTo(LookupRow.OCCURRENCE_ID)).as("o"),
                countIf(col("keyType").equalTo(LookupRow.TRIPLET)).as("t"))
            .filter(col("o").gt(1).or(col("t").gt(1)))
            .count();

    Map<String, Long> metrics = new LinkedHashMap<>();
    for (String name :
        new String[] {
          "rows", "occurrenceIds", "triplets", "missingGbifId", "locked", "distinctGbifIds"
        }) {
      metrics.put(name, totals.getLong(totals.fieldIndex(name)));
    }
    metrics.put("gbifIdsWithMultipleOccurrenceIdsOrTriplets", reused);
    return metrics;
  }

  private static org.apache.spark.sql.Column countIf(org.apache.spark.sql.Column condition) {
    return coalesce(sum(when(condition, 1L).otherwise(0L)), lit(0L));
  }

  static Set<String> readDatasetKeys(SparkSession spark, List<String> ids, String idsFile)
      throws IOException {
    List<String> raw = new ArrayList<>();
    if (ids != null) {
      ids.forEach(k -> Collections.addAll(raw, k.split(",")));
    }
    if (idsFile != null) {
      Path path = new Path(idsFile);
      FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))) {
        reader.lines().forEach(raw::add);
      }
    }
    return parseDatasetKeys(raw);
  }

  static Set<String> parseDatasetKeys(Collection<String> raw) {
    Set<String> keys = new LinkedHashSet<>();
    for (String key : raw) {
      String k = key.trim();
      if (k.isEmpty() || k.startsWith("#")) {
        continue;
      }
      try {
        // fail fast on typos, a wrong key would silently produce an empty export
        keys.add(UUID.fromString(k).toString());
      } catch (IllegalArgumentException ex) {
        throw new IllegalArgumentException("Invalid dataset key: " + k, ex);
      }
    }
    if (keys.isEmpty()) {
      throw new IllegalArgumentException("No dataset keys provided");
    }
    return keys;
  }

  /** Triplets are stored as ic|cc|cn|null, see OccurrenceKeyBuilder#buildKey */
  static String keyType(String uniqueKey) {
    return uniqueKey.endsWith("|null") && uniqueKey.split("\\|", -1).length == 4
        ? LookupRow.TRIPLET
        : LookupRow.OCCURRENCE_ID;
  }

  static LookupRow toLookupRow(String datasetKey, int salt, int prefixLength, Result result) {
    byte[] row = result.getRow();
    String uniqueKey = Bytes.toString(row, prefixLength, row.length - prefixLength);

    Cell idCell = result.getColumnLatestCell(Columns.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN));
    Cell statusCell =
        result.getColumnLatestCell(Columns.CF, Bytes.toBytes(Columns.LOOKUP_STATUS_COLUMN));
    Cell lockCell =
        result.getColumnLatestCell(Columns.CF, Bytes.toBytes(Columns.LOOKUP_LOCK_COLUMN));

    Long gbifId = null;
    if (idCell != null) {
      byte[] value = CellUtil.cloneValue(idCell);
      if (value.length == Bytes.SIZEOF_LONG) {
        gbifId = Bytes.toLong(value);
      }
    }

    return LookupRow.builder()
        .datasetKey(datasetKey)
        .salt(salt)
        .rowKey(Bytes.toStringBinary(row))
        .rowKeyBytes(row)
        .keyType(keyType(uniqueKey))
        .uniqueKey(uniqueKey)
        .gbifId(gbifId)
        .gbifIdTs(idCell == null ? null : idCell.getTimestamp())
        .status(statusCell == null ? null : Bytes.toString(CellUtil.cloneValue(statusCell)))
        .statusTs(statusCell == null ? null : statusCell.getTimestamp())
        .lock(lockCell == null ? null : Bytes.toString(CellUtil.cloneValue(lockCell)))
        .lockTs(lockCell == null ? null : lockCell.getTimestamp())
        .build();
  }

  /** Streams the rows of one dataset in one salt bucket using a bounded prefix scan. */
  static class LookupScanFunction implements FlatMapFunction<Long, LookupRow>, Serializable {

    private final String zkConnection;
    private final String hbaseZnode;
    private final String lookupTable;
    private final String datasetKey;
    private final int scanCaching;

    LookupScanFunction(
        String zkConnection,
        String hbaseZnode,
        String lookupTable,
        String datasetKey,
        int scanCaching) {
      this.zkConnection = zkConnection;
      this.hbaseZnode = hbaseZnode;
      this.lookupTable = lookupTable;
      this.datasetKey = datasetKey;
      this.scanCaching = scanCaching;
    }

    @Override
    public Iterator<LookupRow> call(Long saltValue) throws Exception {
      int salt = saltValue.intValue();
      String prefix = String.format(SALT_FORMAT, salt) + ":" + datasetKey + "|";

      Scan scan =
          new Scan()
              .setStartStopRowForPrefixScan(Bytes.toBytes(prefix))
              .addFamily(Columns.CF)
              .setCaching(scanCaching)
              .setCacheBlocks(false);

      // shared per executor JVM, not closed here
      Connection connection =
          HbaseConnectionFactory.getInstance(zkConnection, hbaseZnode).getConnection();
      Table table = connection.getTable(TableName.valueOf(lookupTable));
      ResultScanner scanner = table.getScanner(scan);

      return new Iterator<LookupRow>() {
        private Result next = advance();

        private Result advance() throws IOException {
          Result r = scanner.next();
          if (r == null) {
            scanner.close();
            table.close();
          }
          return r;
        }

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public LookupRow next() {
          if (next == null) {
            throw new NoSuchElementException();
          }
          LookupRow row = toLookupRow(datasetKey, salt, prefix.length(), next);
          try {
            next = advance();
          } catch (IOException ex) {
            throw new IllegalStateException("Scan of " + prefix + " failed", ex);
          }
          return row;
        }
      };
    }
  }
}
