package org.gbif.pipelines.spark.diag;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.regexp_extract;
import static org.apache.spark.sql.functions.struct;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.Directories;
import org.gbif.pipelines.spark.FragmenterPipeline;
import org.gbif.pipelines.spark.util.FragmentTableUtil;
import org.gbif.pipelines.spark.util.FullBuildUtils;
import org.gbif.pipelines.spark.util.PipelineArgs;
import scala.Tuple2;

/**
 * One-off backfill of the fragment:datasetKey column (and fragment:attempt where missing) on rows
 * of the HBase fragment table written before {@link FragmenterPipeline} recorded them.
 *
 * <p>The fragment row key is salt:gbifId, so the dataset is found by joining each row's gbifId to
 * the final identifiers of every dataset's latest successful attempt in HDFS ({@code
 * <outputPath>/<datasetKey>/<attempt>/identifiers}, where internalId is the gbifId).
 *
 * <ul>
 *   <li>Only rows without a datasetKey are updated, so it is safe to re-run.
 *   <li>Only existing rows are written to - no new rows are created.
 *   <li>An existing attempt cell is left as it is; it is only added when missing (as text, as the
 *       fragmenter writes it).
 *   <li>Rows whose gbifId isn't in any dataset's latest identifiers are left alone, and their row
 *       keys are written to HDFS for review.
 *   <li>{@code --dryRun=true} (the default) only counts and writes the unmatched list; pass {@code
 *       --dryRun=false} to write to HBase.
 * </ul>
 */
@Slf4j
public class FragmentRetrofitPipeline {

  /** Extracts datasetKey (group 1) and attempt (group 2) from an identifiers parquet path. */
  static final String IDENTIFIERS_PATH_PATTERN = "/([^/]+)/(\\d+)/" + Directories.IDENTIFIERS + "/";

  static final StructType FRAGMENT_ROW_SCHEMA =
      new StructType()
          .add("rowKey", DataTypes.StringType, false)
          .add("gbifId", DataTypes.StringType, false)
          .add("hasDatasetKey", DataTypes.BooleanType, false)
          .add("hasAttempt", DataTypes.BooleanType, false);

  @Parameters(separators = "=")
  private static class Args extends PipelineArgs {

    @Parameter(
        names = "--dryRun",
        description =
            "Only count what would be updated and write the unmatched list; don't write to HBase",
        arity = 1)
    private boolean dryRun = true;

    @Parameter(
        names = "--unmatchedOutputPath",
        description =
            "HDFS directory for row keys of fragment rows with no dataset found. Defaults to"
                + " <rebuildPath>/fragment-retrofit-unmatched")
    private String unmatchedOutputPath;
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
    String unmatchedOutputPath =
        args.unmatchedOutputPath != null
            ? args.unmatchedOutputPath
            : config.getRebuildPath() + "/fragment-retrofit-unmatched";

    SparkSession spark =
        getSparkSession(
            args.master,
            "Fragment table datasetKey retrofit" + (args.dryRun ? " (dry run)" : ""),
            config,
            FragmenterPipeline::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);
    try {
      run(spark, fileSystem, config, args.dryRun, unmatchedOutputPath);
    } finally {
      fileSystem.close();
      spark.stop();
    }
  }

  static void run(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      boolean dryRun,
      String unmatchedOutputPath)
      throws Exception {

    long start = System.currentTimeMillis();
    log.info(
        "Starting fragment retrofit of table {} ({})",
        config.getFragmentsTable(),
        dryRun ? "DRY RUN - nothing will be written to HBase" : "WRITING to HBase");

    // 1. gbifId -> datasetKey, attempt from each dataset's latest successful identifiers
    FullBuildUtils.DirectoryScanResult identifierDirs =
        FullBuildUtils.getSuccessfulParquetFilePaths(
            fileSystem,
            config,
            Directories.IDENTIFIERS,
            config.getRebuildPath() + "/fragment-retrofit-datasets-without-identifiers.txt",
            null);
    List<String> identifierPaths = identifierDirs.successfulPaths();
    log.info(
        "Step 1/4: found identifiers for {} datasets (datasets without are listed in {})",
        identifierPaths.size(),
        config.getRebuildPath() + "/fragment-retrofit-datasets-without-identifiers.txt");
    if (identifierPaths.isEmpty()) {
      throw new IllegalStateException(
          "No successful identifiers found under " + config.getOutputPath());
    }

    // if a gbifId appears under more than one dataset, the highest attempt wins
    Dataset<Row> identifiers =
        spark
            .read()
            .parquet(identifierPaths.toArray(new String[0]))
            .select(col("internalId").as("gbifId"), input_file_name().as("path"))
            .where(col("gbifId").isNotNull())
            .withColumn("datasetKey", regexp_extract(col("path"), IDENTIFIERS_PATH_PATTERN, 1))
            .withColumn(
                "attempt",
                regexp_extract(col("path"), IDENTIFIERS_PATH_PATTERN, 2)
                    .cast(DataTypes.IntegerType))
            .groupBy("gbifId")
            .agg(max(struct(col("attempt"), col("datasetKey"))).as("latest"))
            .select(
                col("gbifId"),
                col("latest.datasetKey").as("datasetKey"),
                col("latest.attempt").as("attempt"));

    // 2. fragment rows without a datasetKey (only the datasetKey and attempt columns are read)
    log.info("Step 2/4: scanning fragment table {}", config.getFragmentsTable());
    Configuration scanConf = FragmentTableUtil.scanConfiguration(config);
    JavaRDD<Row> fragmentRows =
        JavaSparkContext.fromSparkContext(spark.sparkContext())
            .newAPIHadoopRDD(
                scanConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
            .map(row -> toFragmentRow(row._2()));
    Dataset<Row> needingDatasetKey =
        spark
            .createDataFrame(fragmentRows, FRAGMENT_ROW_SCHEMA)
            .where(col("hasDatasetKey").equalTo(false));

    // 3. join, and materialise before writing so the scan has finished before any row changes
    Dataset<Row> joined =
        needingDatasetKey
            .join(identifiers, "gbifId", "left")
            .select(
                col("rowKey"), col("gbifId"), col("hasAttempt"), col("datasetKey"), col("attempt"))
            .persist(StorageLevel.MEMORY_AND_DISK_SER());
    try {
      long missing = joined.count();
      long unmatched = joined.where(col("datasetKey").isNull()).count();
      long matched = missing - unmatched;
      long attemptToAdd =
          joined.where(col("datasetKey").isNotNull().and(col("hasAttempt").equalTo(false))).count();
      log.info(
          "Step 3/4: {} fragment rows have no datasetKey: {} matched to a dataset ({} also missing"
              + " an attempt), {} unmatched",
          missing,
          matched,
          attemptToAdd,
          unmatched);

      if (unmatched > 0) {
        joined
            .where(col("datasetKey").isNull())
            .select("rowKey")
            .write()
            .mode(SaveMode.Overwrite)
            .csv(unmatchedOutputPath);
        log.warn(
            "{} fragment rows have a gbifId that isn't in any dataset's latest identifiers; they are"
                + " left unchanged and their row keys are in {}",
            unmatched,
            unmatchedOutputPath);
      }

      // 4. write the datasetKey (and attempt where missing) to the existing rows
      if (dryRun) {
        log.info(
            "Step 4/4: dry run - would update {} fragment rows. Re-run with --dryRun=false",
            matched);
      } else if (matched > 0) {
        log.info("Step 4/4: writing datasetKey to {} fragment rows", matched);
        Configuration outputConf = FragmentTableUtil.hbaseConfiguration(config);
        outputConf.set(TableOutputFormat.OUTPUT_TABLE, config.getFragmentsTable());
        Job job = Job.getInstance(outputConf);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        joined
            .where(col("datasetKey").isNotNull())
            .javaRDD()
            .mapToPair(FragmentRetrofitPipeline::toPut)
            .saveAsNewAPIHadoopDataset(job.getConfiguration());
        log.info("Step 4/4: updated {} fragment rows", matched);
      } else {
        log.info("Step 4/4: nothing to update");
      }
    } finally {
      joined.unpersist();
    }

    log.info(
        "Fragment retrofit finished in {} s ({})",
        (System.currentTimeMillis() - start) / 1000,
        dryRun ? "dry run" : "written");
  }

  /** rowKey, gbifId, hasDatasetKey, hasAttempt for a scanned fragment row (runs on executors). */
  static Row toFragmentRow(Result result) {
    String rowKey = Bytes.toString(result.getRow());
    byte[] attempt = result.getValue(FragmentTableUtil.FAMILY, FragmentTableUtil.ATTEMPT);
    return RowFactory.create(
        rowKey,
        FragmentTableUtil.gbifId(rowKey),
        FragmentTableUtil.datasetKey(result) != null,
        attempt != null && attempt.length > 0);
  }

  /** The Put adding datasetKey, and attempt if the row has none, to an existing fragment row. */
  static Tuple2<ImmutableBytesWritable, Put> toPut(Row row) {
    byte[] rowKey = Bytes.toBytes(row.<String>getAs("rowKey"));
    Put put = new Put(rowKey);
    put.addColumn(
        FragmentTableUtil.FAMILY,
        FragmentTableUtil.DATASET_KEY,
        Bytes.toBytes(row.<String>getAs("datasetKey")));
    if (!row.<Boolean>getAs("hasAttempt")) {
      put.addColumn(
          FragmentTableUtil.FAMILY,
          FragmentTableUtil.ATTEMPT,
          Bytes.toBytes(String.valueOf(row.<Integer>getAs("attempt"))));
    }
    return new Tuple2<>(new ImmutableBytesWritable(rowKey), put);
  }
}
