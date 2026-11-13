package org.gbif.pipelines.spark.diag;

import static org.apache.spark.sql.functions.col;
import static org.gbif.pipelines.core.utils.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.spark.diag.IdentifierExportPipeline.LookupScanFunction;
import org.gbif.pipelines.spark.pojo.LookupRow;
import org.gbif.pipelines.spark.util.PipelineArgs;

/**
 * Deletes the HBase occurrence lookup table rows (triplet/occurrenceID -> gbifID) of one key type
 * ({@link LookupRow#OCCURRENCE_ID} or {@link LookupRow#TRIPLET}) for a list of datasets.
 *
 * <p>Only the lookup rows are removed, the gbifIDs themselves are not reused. After the delete the
 * next ingestion of a dataset can no longer find its records by the deleted key type, so records
 * that are not matched by the other key type get new gbifIDs.
 *
 * <p>Each dataset is read with the same bounded per-salt-bucket prefix scans as {@link
 * IdentifierExportPipeline}. The rows of the requested key type are first written as a backup to
 * {@code <outputPath>/<runId>/<datasetKey>/<keyType>}, and the deletes are issued from that backup,
 * so that every deleted row is recoverable. {@code runId} is the start time of the run, so a re-run
 * never overwrites an earlier backup.
 *
 * <p>The deletes carry the time the scan started, so lookup cells written after that (e.g. by an
 * ingestion running concurrently) are kept. A dataset should nevertheless not be ingested while it
 * is being deleted.
 *
 * <p>{@code --dryRun} defaults to {@code true}: rows are scanned and backed up but nothing is
 * deleted. Pass {@code --dryRun=false} to delete. Counts are written to {@code
 * <outputPath>/<runId>/<datasetKey>/_delete.yml}.
 */
@Slf4j
public class IdentifierDeletePipeline {

  public static final String DEFAULT_OUTPUT_PATH = "/data/identifier-delete";
  public static final String METRICS_FILENAME = "_delete.yml";
  static final List<String> KEY_TYPES = Arrays.asList(LookupRow.OCCURRENCE_ID, LookupRow.TRIPLET);

  static final String OK = IdentifierExportPipeline.OK;
  static final String FAILED = IdentifierExportPipeline.FAILED;

  private static final int NUMBER_OF_BUCKETS = HBaseLockingKeyService.NUMBER_OF_BUCKETS;
  private static final DateTimeFormatter RUN_ID_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

  /**
   * Rejects any key type other than {@link LookupRow#OCCURRENCE_ID} or {@link LookupRow#TRIPLET}.
   */
  public static class KeyTypeValidator implements IParameterValidator {
    @Override
    public void validate(String name, String value) {
      try {
        validateKeyType(value);
      } catch (IllegalArgumentException ex) {
        throw new ParameterException(name + ": " + ex.getMessage());
      }
    }
  }

  @Parameters(separators = "=")
  private static class Args extends PipelineArgs {

    @Parameter(names = "--datasetIds", description = "Comma separated dataset keys")
    private List<String> datasetIds = new ArrayList<>();

    @Parameter(
        names = "--datasetIdsFile",
        description =
            "File with one dataset key per line (blank lines and # comments ignored), "
                + "default filesystem unless a scheme like file:/// is given")
    private String datasetIdsFile;

    @Parameter(
        names = "--keyType",
        description = "Lookup key type to delete: OCCURRENCE_ID or TRIPLET",
        required = true,
        validateWith = KeyTypeValidator.class)
    private String keyType;

    @Parameter(
        names = "--outputPath",
        description = "Backup root directory, a <runId> sub directory is created per run")
    private String outputPath = DEFAULT_OUTPUT_PATH;

    @Parameter(names = "--parallelDatasets", description = "Datasets processed concurrently")
    private int parallelDatasets = 4;

    @Parameter(names = "--scanCaching", description = "Rows fetched per HBase scanner RPC")
    private int scanCaching = 1_000;

    @Parameter(names = "--deleteBatchSize", description = "Deletes sent per HBase RPC")
    private int deleteBatchSize = 1_000;

    @Parameter(
        names = "--dryRun",
        description = "Only scan and back up the rows that would be deleted",
        arity = 1)
    private boolean dryRun = true;
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

      Set<String> datasetKeys =
          IdentifierExportPipeline.readDatasetKeys(spark, args.datasetIds, args.datasetIdsFile);

      String runPath = args.outputPath + "/" + LocalDateTime.now().format(RUN_ID_FORMAT);

      result =
          delete(
              spark,
              fileSystem,
              config,
              datasetKeys,
              args.keyType,
              runPath,
              args.parallelDatasets,
              args.scanCaching,
              args.deleteBatchSize,
              args.dryRun);
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
      log.error("{} datasets failed", failed);
      System.exit(1);
    }
    System.exit(0);
  }

  /**
   * Backs up and (unless dryRun) deletes the lookup rows of the given key type of the given
   * datasets.
   *
   * @param runPath directory the backups and metrics of this run are written to
   * @return status (OK or FAILED) per dataset key
   */
  public static Map<String, String> delete(
      SparkSession spark,
      FileSystem fs,
      PipelinesConfig config,
      Collection<String> datasetKeys,
      String keyType,
      String runPath,
      int parallelDatasets,
      int scanCaching,
      int deleteBatchSize,
      boolean dryRun)
      throws Exception {

    validateKeyType(keyType);

    long start = System.currentTimeMillis();
    log.info(
        "{} {} lookup rows of {} datasets from {}, backups in {}, {} at a time",
        dryRun ? "DRY RUN - backing up" : "Deleting",
        keyType,
        datasetKeys.size(),
        config.getKeygen().getLookupTable(),
        runPath,
        parallelDatasets);

    Map<String, Future<String>> futures = new LinkedHashMap<>();
    ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, parallelDatasets));
    try {
      for (String datasetKey : datasetKeys) {
        futures.put(
            datasetKey,
            executor.submit(
                () ->
                    deleteDataset(
                        spark,
                        fs,
                        config,
                        datasetKey,
                        keyType,
                        runPath,
                        scanCaching,
                        deleteBatchSize,
                        dryRun)));
      }
      executor.shutdown();

      Map<String, String> result = new LinkedHashMap<>();
      for (Map.Entry<String, Future<String>> e : futures.entrySet()) {
        result.put(e.getKey(), e.getValue().get());
      }

      log.info(
          "Finished in {} secs: {} OK, {} failed {}",
          (System.currentTimeMillis() - start) / 1000,
          result.values().stream().filter(OK::equals).count(),
          result.values().stream().filter(FAILED::equals).count(),
          result);
      return result;
    } finally {
      executor.shutdownNow();
    }
  }

  private static String deleteDataset(
      SparkSession spark,
      FileSystem fs,
      PipelinesConfig config,
      String datasetKey,
      String keyType,
      String runPath,
      int scanCaching,
      int deleteBatchSize,
      boolean dryRun) {

    Path datasetDir = new Path(runPath, datasetKey);
    Path backupDir = new Path(datasetDir, keyType);
    long start = System.currentTimeMillis();

    try {
      spark
          .sparkContext()
          .setJobGroup(datasetKey, "Delete " + keyType + " identifiers " + datasetKey, false);

      // 1. back up the rows to delete - one scan task per salt bucket
      long deleteTimestamp = System.currentTimeMillis();
      scan(spark, config, datasetKey, keyType, scanCaching)
          .write()
          .mode(SaveMode.ErrorIfExists)
          .parquet(backupDir.toString());

      Dataset<LookupRow> backup =
          spark.read().parquet(backupDir.toString()).as(Encoders.bean(LookupRow.class));

      Map<String, Long> metrics = new LinkedHashMap<>();
      metrics.put("rows", backup.count());
      metrics.put("dryRun", dryRun ? 1L : 0L);

      // 2. delete the backed up rows
      if (!dryRun) {
        LongAccumulator deleted = spark.sparkContext().longAccumulator("deleted-" + datasetKey);
        backup.foreachPartition(
            new LookupDeleteFunction(
                config.getKeygen().getZkConnectionString(),
                config.getKeygen().getHbaseZnode(),
                config.getKeygen().getLookupTable(),
                deleteTimestamp,
                deleteBatchSize,
                deleted));
        metrics.put("deleted", deleted.value());
        log.info(
            "Deleted {} of {} {} lookup rows of dataset {}",
            deleted.value(),
            metrics.get("rows"),
            keyType,
            datasetKey);

        // 3. verify, rows written after the scan started are expected to remain
        metrics.put("remaining", scan(spark, config, datasetKey, keyType, scanCaching).count());
      } else {
        log.info(
            "DRY RUN - {} {} lookup rows of dataset {} would be deleted",
            metrics.get("rows"),
            keyType,
            datasetKey);
      }

      writeMetricsYaml(fs, metrics, new Path(datasetDir, METRICS_FILENAME).toString());

      log.info(
          "{} {} {} rows of dataset {} in {} secs: {}",
          dryRun ? "Backed up (dry run)" : "Deleted",
          dryRun ? metrics.get("rows") : metrics.get("deleted"),
          keyType,
          datasetKey,
          (System.currentTimeMillis() - start) / 1000,
          metrics);
      return OK;

    } catch (Exception ex) {
      log.error("Deleting {} identifiers of dataset {} failed", keyType, datasetKey, ex);
      return FAILED;
    } finally {
      spark.sparkContext().clearJobGroup();
    }
  }

  private static Dataset<LookupRow> scan(
      SparkSession spark,
      PipelinesConfig config,
      String datasetKey,
      String keyType,
      int scanCaching) {
    LookupScanFunction scanFn =
        new LookupScanFunction(
            config.getKeygen().getZkConnectionString(),
            config.getKeygen().getHbaseZnode(),
            config.getKeygen().getLookupTable(),
            datasetKey,
            scanCaching);

    return spark
        .range(0, NUMBER_OF_BUCKETS, 1, NUMBER_OF_BUCKETS)
        .flatMap(scanFn, Encoders.bean(LookupRow.class))
        .filter(col("keyType").equalTo(keyType));
  }

  /** @throws IllegalArgumentException unless keyType is OCCURRENCE_ID or TRIPLET */
  static void validateKeyType(String keyType) {
    if (!KEY_TYPES.contains(keyType)) {
      throw new IllegalArgumentException(
          "Invalid key type [" + keyType + "], must be one of " + KEY_TYPES);
    }
  }

  /** Deletes the lookup rows of a partition in batches. */
  static class LookupDeleteFunction implements ForeachPartitionFunction<LookupRow>, Serializable {

    private final String zkConnection;
    private final String hbaseZnode;
    private final String lookupTable;
    private final long deleteTimestamp;
    private final int batchSize;
    private final LongAccumulator deleted;

    LookupDeleteFunction(
        String zkConnection,
        String hbaseZnode,
        String lookupTable,
        long deleteTimestamp,
        int batchSize,
        LongAccumulator deleted) {
      this.zkConnection = zkConnection;
      this.hbaseZnode = hbaseZnode;
      this.lookupTable = lookupTable;
      this.deleteTimestamp = deleteTimestamp;
      this.batchSize = Math.max(1, batchSize);
      this.deleted = deleted;
    }

    @Override
    public void call(Iterator<LookupRow> rows) throws Exception {
      // shared per executor JVM, not closed here
      Connection connection =
          HbaseConnectionFactory.getInstance(zkConnection, hbaseZnode).getConnection();
      long partitionDeletes = 0;
      try (Table table = connection.getTable(TableName.valueOf(lookupTable))) {
        List<Delete> batch = new ArrayList<>(batchSize);
        while (rows.hasNext()) {
          // removes all cells of the row written up to the time the scan started
          batch.add(new Delete(rows.next().getRowKeyBytes(), deleteTimestamp));
          if (batch.size() >= batchSize) {
            partitionDeletes += flush(table, batch);
          }
        }
        partitionDeletes += flush(table, batch);
      }
      if (partitionDeletes > 0) {
        log.info("Deleted {} rows from {} in this partition", partitionDeletes, lookupTable);
      }
    }

    /** @return the number of deletes sent */
    private int flush(Table table, List<Delete> batch) throws IOException {
      if (batch.isEmpty()) {
        return 0;
      }
      int size = batch.size();
      // throws if any delete fails, failed deletes are left in the list
      table.delete(batch);
      deleted.add(size);
      batch.clear();
      return size;
    }
  }
}
