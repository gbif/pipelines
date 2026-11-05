package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.util.FullBuildUtils.checkDatasetTypeSupported;
import static org.gbif.pipelines.spark.util.LogUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.RecordsTableConfig;
import org.gbif.pipelines.spark.records.RecordsTableWriter;
import org.gbif.pipelines.spark.records.RecordsTableWriter.RecordType;
import org.gbif.pipelines.spark.records.RecordsTableWriter.RecordsLoad;
import org.gbif.pipelines.spark.util.FullBuildUtils;
import org.gbif.pipelines.spark.util.PipelineArgs;

/**
 * Rebuilds the HBase records table of occurrences or events (see docs/hbase-records-tables.md) from
 * the last successful interpretation of every dataset, without touching Elasticsearch. It reads the
 * same parquet as {@link FullIndexBuildPipeline} and loads the datasets in batches of about {@code
 * --batchRecords} records, writing a new manifest for each. Each batch is written, bulk loaded and
 * committed before the next one starts, which bounds the shuffle, the spills and the HFiles staged
 * at once. After a failure, {@code --startBatch} resumes from the batch that failed: the batches
 * are the same as long as the source parquet is.
 *
 * <p>With {@code --truncate=true} the table is emptied, keeping its regions, and its manifests
 * removed before the load. Without it, the records are replaced and the records no longer in the
 * datasets loaded are deleted, as an incremental load does.
 *
 * <p>With {@code --createTable=true} the table is created, pre-split into {@code --regions}
 * regions, and its manifests removed, the job then is the only one using it. The table must not
 * exist.
 *
 * <p>The compactions of the table are disabled while the batches load, so each batch's HFiles
 * aren't rewritten while the next one is written. Once the last batch is loaded they are enabled
 * and a major compaction requested. A failed build leaves them disabled until a resumed one
 * completes.
 *
 * <p>{@code --table} and {@code --manifestPath} build a new, pre-split table next to the live one,
 * to point the configuration at once built.
 */
@Slf4j
public class FullRecordsTableBuildPipeline {

  @Parameters(separators = "=")
  private static class Args extends PipelineArgs {

    @Parameter(names = NUMBER_OF_SHARDS_ARG, description = "Number of shards")
    private int numberOfShards = 2400;

    @Parameter(
        names = SOURCE_DIRECTORY_ARG,
        description = "Directory containing the parquet to load")
    private String sourceDirectory = "json";

    @Parameter(names = DATASET_TYPE_ARG, description = "OCCURRENCE or SAMPLING_EVENT")
    private DatasetType datasetType = DatasetType.OCCURRENCE;

    @Parameter(
        names = UNSUCCESSFUL_DUMP_FILENAME,
        description =
            "Filename to dump the list of unsuccessful datasets to in HDFS for later review")
    private String unsuccessfulDumpFilename = "unsuccessful-records-table-datasets.txt";

    @Parameter(
        names = "--earliestModificationTime",
        description =
            "Only consider parquet files modified after this time (ISO 8601 format, e.g. 2024-01-01T00:00:00Z)")
    private String earliestModificationTime = null;

    @Parameter(
        names = "--truncate",
        description =
            "Empty the table, keeping its regions, and remove its manifests before loading. "
                + "The API serves no records from the table until the load completes.",
        arity = 1)
    private boolean truncate = false;

    @Parameter(
        names = "--batchRecords",
        description =
            "Approximate number of records per batch, a dataset with more records is a batch of its own")
    private long batchRecords = 100_000_000L;

    @Parameter(
        names = "--startBatch",
        description = "First batch to load, starting from 0, to resume a build that failed")
    private int startBatch = 0;

    @Parameter(
        names = "--createTable",
        description =
            "Create the table, pre-split and empty, and remove its manifests. The table must not exist.",
        arity = 1)
    private boolean createTable = false;

    @Parameter(
        names = "--regions",
        description =
            "Regions of the created table, 100 for occurrences (one per salt, at most 100) and 16 for events by default")
    private Integer regions = null;

    @Parameter(
        names = "--table",
        description =
            "HBase table to build instead of the configured one, e.g. a new pre-split table")
    private String table = null;

    @Parameter(
        names = "--manifestPath",
        description =
            "Manifest directory to use instead of the configured one, required with --table, "
                + "as the manifests describe the keys of one table")
    private String manifestPath = null;
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

    checkDatasetTypeSupported(args.datasetType);

    PipelinesConfig config = loadConfig(args.config);
    RecordType type =
        args.datasetType == DatasetType.OCCURRENCE ? RecordType.OCCURRENCE : RecordType.EVENT;
    overrideTable(config.getRecordsTableConfig(), type, args.table, args.manifestPath);

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master, "Rebuild records table - " + args.datasetType, config, (builder, c) -> {});
    FileSystem fileSystem = getFileSystem(spark, config);
    /* ############ standard init block - end ########## */

    run(spark, fileSystem, config, RecordsTableWriter.hbaseConfiguration(config), type, args);

    fileSystem.close();
    spark.stop();
    spark.close();
  }

  private static void run(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      Configuration hbaseConf,
      RecordType type,
      Args args)
      throws IOException {

    long start = System.currentTimeMillis();

    FullBuildUtils.DirectoryScanResult scanResult =
        FullBuildUtils.getSuccessfulParquetFilePaths(
            fileSystem,
            config,
            args.sourceDirectory,
            config.getRebuildPath() + "/" + args.unsuccessfulDumpFilename,
            args.earliestModificationTime);

    if (scanResult.successfulPaths().isEmpty()) {
      log.warn("No datasets with successful interpretations found. Exiting.");
      return;
    }

    log.info(
        "Building {} records table {} from {} datasets",
        type,
        type.table(config.getRecordsTableConfig()),
        scanResult.datasetAttemptMap().size());

    if ((args.truncate || args.createTable) && args.startBatch > 0) {
      throw new IllegalArgumentException(
          "--truncate and --createTable would remove the batches before --startBatch, resume without them");
    }
    if (args.truncate && args.createTable) {
      throw new IllegalArgumentException(
          "--createTable creates an empty table, don't --truncate it");
    }
    if (args.createTable) {
      int regions = args.regions != null ? args.regions : type == RecordType.OCCURRENCE ? 100 : 16;
      RecordsTableWriter.createTable(fileSystem, config, hbaseConf, type, regions);
    } else if (args.truncate) {
      RecordsTableWriter.truncate(fileSystem, config, hbaseConf, type);
    }
    RecordsTableWriter.compactions(config, hbaseConf, type, false);

    // only the dataset key column is read to count, a cheap pass over the parquet
    Map<String, Long> recordCounts =
        spark
            .read()
            .parquet(scanResult.successfulPaths().toArray(new String[0]))
            .groupBy(RecordsTableWriter.DOCUMENT_DATASET_KEY)
            .count()
            .collectAsList()
            .stream()
            .collect(Collectors.toMap(r -> r.getString(0), r -> r.getLong(1)));

    List<Map<String, Integer>> batches =
        batches(scanResult.datasetAttemptMap(), recordCounts, args.batchRecords);
    log.info(
        "Loading {} records of {} datasets in {} batches, starting from batch {}",
        recordCounts.values().stream().mapToLong(Long::longValue).sum(),
        scanResult.datasetAttemptMap().size(),
        batches.size(),
        args.startBatch);

    Map<String, String> datasetPaths = datasetPaths(scanResult.successfulPaths());
    long loaded = 0;
    long removed = 0;
    for (int i = args.startBatch; i < batches.size(); i++) {
      Map<String, Integer> batch = batches.get(i);
      long batchStart = System.currentTimeMillis();
      log.info("Loading batch {} of {}, {} datasets", i, batches.size(), batch.size());

      String[] paths =
          batch.keySet().stream()
              .map(datasetPaths::get)
              .filter(Objects::nonNull)
              .toArray(String[]::new);
      Dataset<Row> documents = spark.read().parquet(paths).coalesce(args.numberOfShards);

      RecordsLoad load =
          RecordsTableWriter.load(
              spark,
              fileSystem,
              config,
              hbaseConf,
              type,
              batch,
              documents,
              config.getRebuildPath() + "/records-" + type.name().toLowerCase());

      // nothing is indexed here, the records removed can be deleted straight away
      removed += load.commit();
      loaded += load.getLoaded();
      log.info(
          "Batch {} of {}: {}",
          i,
          batches.size(),
          timeAndRecPerSecond("full-records-table-build", batchStart, load.getLoaded()));
    }

    log.info("Loaded {} records and removed {}", loaded, removed);
    RecordsTableWriter.compactions(config, hbaseConf, type, true);
    RecordsTableWriter.majorCompact(config, hbaseConf, type);
    log.info(timeAndRecPerSecond("full-records-table-build", start, loaded));
  }

  /**
   * Groups the datasets, in key order, into batches of up to {@code batchRecords} records. A
   * dataset with more records is a batch of its own. The datasets without records join the batch
   * being built, so their records are removed as part of it.
   *
   * @return the attempt of each dataset of each batch
   */
  static List<Map<String, Integer>> batches(
      Map<String, Integer> datasetAttempts, Map<String, Long> recordCounts, long batchRecords) {
    List<Map<String, Integer>> batches = new ArrayList<>();
    Map<String, Integer> batch = new TreeMap<>();
    long batchCount = 0;
    for (String datasetKey : new TreeSet<>(datasetAttempts.keySet())) {
      long count = recordCounts.getOrDefault(datasetKey, 0L);
      if (count > 0 && batchCount > 0 && batchCount + count > batchRecords) {
        batches.add(batch);
        batch = new TreeMap<>();
        batchCount = 0;
      }
      batch.put(datasetKey, datasetAttempts.get(datasetKey));
      batchCount += count;
    }
    if (!batch.isEmpty()) {
      batches.add(batch);
    }
    return batches;
  }

  /**
   * Source path of each dataset, the paths are {@code
   * <outputPath>/<datasetKey>/<attempt>/<source>/}
   */
  static Map<String, String> datasetPaths(List<String> paths) {
    return paths.stream()
        .collect(Collectors.toMap(p -> new Path(p).getParent().getParent().getName(), p -> p));
  }

  /** Points the record type at another table and its own manifests */
  static void overrideTable(
      RecordsTableConfig config, RecordType type, String table, String manifestPath) {
    if (table != null && manifestPath == null) {
      throw new IllegalArgumentException(
          "--manifestPath is required with --table, the configured manifests are of another table");
    }
    if (table != null) {
      if (type == RecordType.OCCURRENCE) {
        config.setOccurrenceTable(table);
      } else {
        config.setEventTable(table);
      }
    }
    if (manifestPath != null) {
      config.setManifestPath(manifestPath);
    }
  }
}
