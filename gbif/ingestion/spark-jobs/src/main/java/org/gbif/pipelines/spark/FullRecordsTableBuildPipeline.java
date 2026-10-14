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
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
 * same parquet as {@link FullIndexBuildPipeline} and loads all the datasets in one bulk load,
 * writing a new manifest for each.
 *
 * <p>With {@code --truncate=true} the table is emptied, keeping its regions, and its manifests
 * removed before the load. Without it, the records are replaced and the records no longer in the
 * datasets loaded are deleted, as an incremental load does.
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

    if (args.truncate) {
      RecordsTableWriter.truncate(fileSystem, config, hbaseConf, type);
    }

    Dataset<Row> documents =
        spark
            .read()
            .parquet(scanResult.successfulPaths().toArray(new String[0]))
            .coalesce(args.numberOfShards);

    RecordsLoad load =
        RecordsTableWriter.load(
            spark,
            fileSystem,
            config,
            hbaseConf,
            type,
            scanResult.datasetAttemptMap(),
            documents,
            config.getRebuildPath() + "/records-" + type.name().toLowerCase());

    // nothing is indexed here, the records removed can be deleted straight away
    load.commit();
    log.info(timeAndRecPerSecond("full-records-table-build", start, load.getLoaded()));
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
