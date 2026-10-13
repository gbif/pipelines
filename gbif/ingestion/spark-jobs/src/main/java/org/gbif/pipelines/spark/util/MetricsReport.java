package org.gbif.pipelines.spark.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.gbif.pipelines.spark.IdentifiersPipeline;
import org.gbif.pipelines.spark.TableBuildPipeline;

/**
 * Produces a single CSV report of ingestion metrics for every dataset. For each dataset directory
 * under the configured output path, the latest attempt containing a {@code
 * verbatim-to-identifier.yml} is used. Its metrics, plus those of the other step metrics files in
 * {@link #METRICS_FILES} from the same attempt (or NOT_AVAILABLE where a file is missing), are
 * written as one CSV row to HDFS (default directory {@code /tmp}).
 *
 * <p>Optionally (disabled by default) the report can also include:
 *
 * <ul>
 *   <li>{@code --esCounts=true}: the per-dataset record count from the occurrence search API
 *       (Elasticsearch), column {@value #ES_COUNT_COLUMN}
 *   <li>{@code --icebergCounts=true}: the per-dataset record count from the {@code occurrence}
 *       table via Spark SQL, column {@value #ICEBERG_COUNT_COLUMN}
 * </ul>
 */
@Slf4j
public class MetricsReport {

  static final String NOT_AVAILABLE = "NOT_AVAILABLE";
  static final String ES_COUNT_COLUMN = "EsOccurrenceIndexCount";
  static final String ICEBERG_COUNT_COLUMN = "IceberOccurrenceIndexCount";
  static final String ES_FACET_PATH =
      "/occurrence/search?limit=0&facet=datasetKey&facetLimit=120000";
  /** How often (in datasets) to log report-writing progress. */
  static final int PROGRESS_INTERVAL = 500;

  /** The CSV row for one dataset, plus what was missing, so counts can be tallied in order. */
  record DatasetRow(
      String row, List<String> missingFiles, boolean esMissing, boolean icebergMissing) {}

  static final String ICEBERG_COUNT_SQL =
      "SELECT datasetKey, count(*) AS cnt FROM occurrence GROUP BY datasetKey";

  /** A metrics YAML file in the attempt directory, and the metric keys to report from it. */
  record MetricsFile(String filename, List<String> columns) {}

  /**
   * The metrics files to report on, in column order. The identifier metrics file determines the
   * attempt; the others are read from that same attempt directory when available.
   */
  static final List<MetricsFile> METRICS_FILES =
      List.of(
          new MetricsFile(
              IdentifiersPipeline.METRICS_FILENAME,
              List.of(
                  "gbifIdRecordsCountAttempted",
                  "uniqueIdsCountAttempted",
                  "validAndPersistedGbifIdCountAttempted",
                  "absentGbifIdCountAttempted",
                  "invalidGbifIdCountAttempted",
                  "filteredGbifIdsCountAttempted",
                  "duplicatedIdsCountAttempted")),
          new MetricsFile(
              "archive-to-verbatim.yml",
              List.of("archiveToErCount", "archiveToOccurrenceCount", "archiveToLargestFileCount")),
          new MetricsFile(
              "verbatim-to-occurrence.yml", List.of("basicRecordsCount", "uniqueGbifIdsCount")),
          new MetricsFile("occurrence-to-hdfs.yml", List.of("avroToHdfsCountAttempted")),
          new MetricsFile("occurrence-to-index.yml", List.of("parquetToJsonCountAttempted")));

  @Parameters(separators = "=")
  private static class Args extends PipelineArgs {

    @Parameter(names = "--outputDir", description = "HDFS directory to write the CSV report to")
    private String outputDir = "/tmp";

    @Parameter(
        names = "--outputFilename",
        description = "Report filename. Defaults to verbatim-to-identifier-report-<timestamp>.csv")
    private String outputFilename;

    @Parameter(
        names = "--esCounts",
        description =
            "Add the per-dataset occurrence count from the occurrence search API (Elasticsearch)",
        arity = 1)
    private boolean esCounts = false;

    @Parameter(
        names = "--icebergCounts",
        description = "Add the per-dataset occurrence count from the occurrence table (Spark SQL)",
        arity = 1)
    private boolean icebergCounts = false;

    @Parameter(
        names = "--threads",
        description =
            "Number of threads reading the metrics files from HDFS (the work is I/O bound, so this"
                + " can be well above the number of cores)")
    private int threads = 20;
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

    long start = System.currentTimeMillis();
    log.info(
        "Starting report: config={}, outputDir={}, esCounts={}, icebergCounts={}",
        args.config,
        args.outputDir,
        args.esCounts,
        args.icebergCounts);

    PipelinesConfig config = loadConfig(args.config);
    log.info("Loaded config, scanning output path {}", config.getOutputPath());

    Map<String, Long> esCounts = null;
    if (args.esCounts) {
      log.info("Step 1/3: fetching Elasticsearch counts");
      esCounts = fetchEsCounts(config);
    } else {
      log.info("Step 1/3: Elasticsearch counts disabled (--esCounts=true to enable)");
    }

    Map<String, Long> icebergCounts = null;
    if (args.icebergCounts) {
      log.info("Step 2/3: fetching occurrence table counts");
      icebergCounts = fetchIcebergCounts(config, args.master);
    } else {
      log.info("Step 2/3: occurrence table counts disabled (--icebergCounts=true to enable)");
    }

    Configuration hadoopConf = new Configuration();
    hadoopConf.addResource(new Path(config.getHdfsSiteConfig()));
    hadoopConf.addResource(new Path(config.getCoreSiteConfig()));
    FileSystem fileSystem = FileSystem.get(hadoopConf);

    String filename =
        args.outputFilename != null
            ? args.outputFilename
            : "verbatim-to-identifier-report-" + System.currentTimeMillis() + ".csv";
    Path reportPath = new Path(args.outputDir, filename);

    log.info("Step 3/3: scanning datasets and writing report to {}", reportPath);
    int rows = writeReport(fileSystem, config, reportPath, esCounts, icebergCounts, args.threads);
    log.info(
        "Finished: wrote metrics for {} datasets to {} in {} s",
        rows,
        reportPath,
        (System.currentTimeMillis() - start) / 1000);
  }

  /**
   * Calls the occurrence search API with a datasetKey facet and returns datasetKey to count.
   *
   * @throws IOException if the request fails or the response isn't the expected shape
   */
  static Map<String, Long> fetchEsCounts(PipelinesConfig config)
      throws IOException, InterruptedException {

    WsConfig api = config.getGbifApi();
    String baseUrl = api.getWsUrl().replaceAll("/+$", "");
    URI uri = URI.create(baseUrl + ES_FACET_PATH);
    log.info("Fetching Elasticsearch dataset counts from {}", uri);
    long start = System.currentTimeMillis();

    HttpRequest.Builder request =
        HttpRequest.newBuilder(uri)
            .timeout(Duration.ofSeconds(Math.max(api.getTimeoutSec(), 300)))
            .header("Accept", "application/json")
            .GET();
    if (api.getHttpHeaders() != null) {
      api.getHttpHeaders().forEach(request::header);
    }

    HttpResponse<String> response =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(api.getTimeoutSec()))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build()
            .send(request.build(), HttpResponse.BodyHandlers.ofString(UTF_8));

    log.info(
        "Occurrence search responded HTTP {} with {} bytes in {} ms",
        response.statusCode(),
        response.body() == null ? 0 : response.body().length(),
        System.currentTimeMillis() - start);
    if (response.statusCode() != 200) {
      throw new IOException(
          "Occurrence search returned HTTP " + response.statusCode() + " for " + uri);
    }
    return parseEsFacetCounts(response.body());
  }

  /** Parses the datasetKey facet counts from an occurrence search response. */
  static Map<String, Long> parseEsFacetCounts(String json) throws IOException {
    JsonNode root = new ObjectMapper().readTree(json);
    Map<String, Long> counts = new HashMap<>();
    for (JsonNode facet : root.path("facets")) {
      if ("DATASET_KEY".equalsIgnoreCase(facet.path("field").asText())) {
        for (JsonNode count : facet.path("counts")) {
          counts.put(count.path("name").asText(), count.path("count").asLong());
        }
      }
    }
    if (counts.isEmpty()) {
      log.warn("No DATASET_KEY facet counts found in occurrence search response");
    }
    log.info(
        "Fetched Elasticsearch counts for {} datasets (total count in response: {})",
        counts.size(),
        root.path("count").asLong());
    return counts;
  }

  /**
   * Hive support (as {@link TableBuildPipeline}) plus the Iceberg session catalog, so that Iceberg
   * tables registered in the Hive metastore (e.g. occurrence) are read through Iceberg. Without
   * this Spark treats them as plain Hive tables and fails trying to instantiate their abstract
   * {@code FileInputFormat} (InstantiationException). These match the spark-defaults used by the
   * other pipeline deployments; the Iceberg runtime jar must be on the classpath.
   */
  static void configIcebergSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    TableBuildPipeline.configSparkSession(sparkBuilder, config);
    sparkBuilder
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
        .config("spark.sql.catalog.local.commit.retry.num-retries", 10)
        .config("spark.sql.catalog.local.commit.retry.min-wait-ms", 100)
        .config("spark.sql.catalog.local.commit.retry.max-wait-ms", 1000)
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        // the default catalog must be defined, or Spark fails with CatalogNotFoundException
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic");

    if (config.getHiveMetastoreUris() != null) {
      sparkBuilder.config("spark.sql.catalog.spark_catalog.uri", config.getHiveMetastoreUris());
      sparkBuilder.config("spark.sql.catalog.iceberg.uri", config.getHiveMetastoreUris());
    }
  }

  /** Runs a group-by count on the occurrence table and returns datasetKey to count. */
  static Map<String, Long> fetchIcebergCounts(PipelinesConfig config, String master) {
    long start = System.currentTimeMillis();
    log.info("Starting Spark session (metastore {})", config.getHiveMetastoreUris());
    SparkSession spark =
        getSparkSession(
            master,
            "Identifier metrics report - occurrence table counts",
            config,
            MetricsReport::configIcebergSparkSession);
    try {
      log.info(
          "Spark session started in {} ms (defaultCatalog={}, spark_catalog={}), using database {}",
          System.currentTimeMillis() - start,
          spark.conf().get("spark.sql.defaultCatalog", "<default>"),
          spark.conf().get("spark.sql.catalog.spark_catalog", "<default>"),
          config.getHiveDB());
      spark.sql("USE " + config.getHiveDB());
      log.info("Running: {}", ICEBERG_COUNT_SQL);
      long queryStart = System.currentTimeMillis();
      Map<String, Long> counts = new HashMap<>();
      for (Row row : spark.sql(ICEBERG_COUNT_SQL).collectAsList()) {
        if (!row.isNullAt(0)) {
          counts.put(row.getString(0), row.getLong(1));
        }
      }
      log.info(
          "Fetched occurrence table counts for {} datasets, query took {} s",
          counts.size(),
          (System.currentTimeMillis() - queryStart) / 1000);
      return counts;
    } finally {
      log.info("Stopping Spark session");
      spark.stop();
    }
  }

  /**
   * Scans for the latest verbatim-to-identifier.yml per dataset and writes the CSV report.
   *
   * @param esCounts datasetKey to Elasticsearch count, or null to omit the column
   * @param icebergCounts datasetKey to occurrence table count, or null to omit the column
   * @param threads number of threads reading the metrics files
   * @return the number of dataset rows written
   */
  public static int writeReport(
      FileSystem fileSystem,
      PipelinesConfig config,
      Path reportPath,
      Map<String, Long> esCounts,
      Map<String, Long> icebergCounts,
      int threads)
      throws IOException {

    FullBuildUtils.DirectoryScanResult scanResult =
        FullBuildUtils.getLatestAttemptFilePaths(
            fileSystem, config, IdentifiersPipeline.METRICS_FILENAME);

    log.info(
        "Found {} datasets with {}",
        scanResult.datasetAttemptMap().size(),
        IdentifiersPipeline.METRICS_FILENAME);

    // sort by dataset UUID for a stable report
    Map<String, Integer> datasetAttempts = new TreeMap<>(scanResult.datasetAttemptMap());

    int total = datasetAttempts.size();
    long start = System.currentTimeMillis();
    // per metrics file: how many datasets had it missing (NOT_AVAILABLE)
    Map<String, Integer> missingCounts = new java.util.LinkedHashMap<>();
    METRICS_FILES.forEach(f -> missingCounts.put(f.filename(), 0));
    int esMissing = 0;
    int icebergMissing = 0;

    int rows = 0;
    try (FSDataOutputStream out = fileSystem.create(reportPath, true);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, UTF_8))) {

      StringBuilder header = new StringBuilder("datasetUUID,attempt");
      for (MetricsFile metricsFile : METRICS_FILES) {
        for (String column : metricsFile.columns()) {
          header.append(',').append(column);
        }
      }
      if (esCounts != null) {
        header.append(',').append(ES_COUNT_COLUMN);
      }
      if (icebergCounts != null) {
        header.append(',').append(ICEBERG_COUNT_COLUMN);
      }
      writer.write(header.toString());
      writer.newLine();

      // Build each dataset's row on a thread pool (the work is HDFS round trips), then write the
      // rows in submission order so the report stays sorted by dataset UUID. Counters are
      // tallied on this thread only, while writing.
      int poolSize = Math.max(1, Math.min(threads, Math.max(1, total)));
      log.info("Reading metrics for {} datasets using {} threads", total, poolSize);
      ExecutorService executor = Executors.newFixedThreadPool(poolSize);
      AtomicInteger completed = new AtomicInteger();
      try {
        List<Future<DatasetRow>> futures = new java.util.ArrayList<>(total);
        for (Map.Entry<String, Integer> entry : datasetAttempts.entrySet()) {
          String datasetId = entry.getKey();
          int attempt = entry.getValue();
          futures.add(
              executor.submit(
                  () -> {
                    DatasetRow datasetRow =
                        buildRow(fileSystem, config, datasetId, attempt, esCounts, icebergCounts);
                    logProgress(completed.incrementAndGet(), total, start);
                    return datasetRow;
                  }));
        }

        for (Future<DatasetRow> future : futures) {
          DatasetRow datasetRow = getRow(future);
          for (String file : datasetRow.missingFiles()) {
            missingCounts.merge(file, 1, Integer::sum);
          }
          if (datasetRow.esMissing()) {
            esMissing++;
          }
          if (datasetRow.icebergMissing()) {
            icebergMissing++;
          }
          writer.write(datasetRow.row());
          writer.newLine();
          rows++;
          log.debug("Row {}/{}: {}", rows, total, datasetRow.row());
        }
      } finally {
        executor.shutdownNow();
      }
    }

    log.info("Report written: {} rows in {} s", rows, (System.currentTimeMillis() - start) / 1000);
    for (Map.Entry<String, Integer> missing : missingCounts.entrySet()) {
      log.info(
          "  {}: available for {}, NOT_AVAILABLE for {}",
          missing.getKey(),
          rows - missing.getValue(),
          missing.getValue());
    }
    if (esCounts != null) {
      log.info("  {}: {} datasets not in Elasticsearch (written as 0)", ES_COUNT_COLUMN, esMissing);
    }
    if (icebergCounts != null) {
      log.info(
          "  {}: {} datasets not in occurrence table (written as 0)",
          ICEBERG_COUNT_COLUMN,
          icebergMissing);
    }
    return rows;
  }

  /** Builds the CSV row for one dataset. Thread safe: only reads from HDFS and the count maps. */
  private static DatasetRow buildRow(
      FileSystem fileSystem,
      PipelinesConfig config,
      String datasetId,
      int attempt,
      Map<String, Long> esCounts,
      Map<String, Long> icebergCounts)
      throws IOException {

    String attemptDir =
        String.join("/", config.getOutputPath(), datasetId, String.valueOf(attempt));

    StringBuilder row = new StringBuilder(datasetId).append(',').append(attempt);
    List<String> missingFiles = new java.util.ArrayList<>();
    for (MetricsFile metricsFile : METRICS_FILES) {
      boolean found =
          appendMetrics(fileSystem, attemptDir + "/" + metricsFile.filename(), metricsFile, row);
      if (!found) {
        missingFiles.add(metricsFile.filename());
      }
    }

    // a dataset absent from the facet / group-by has no records there
    boolean esMissing = false;
    if (esCounts != null) {
      esMissing = !esCounts.containsKey(datasetId);
      row.append(',').append(esCounts.getOrDefault(datasetId, 0L));
    }
    boolean icebergMissing = false;
    if (icebergCounts != null) {
      icebergMissing = !icebergCounts.containsKey(datasetId);
      row.append(',').append(icebergCounts.getOrDefault(datasetId, 0L));
    }
    return new DatasetRow(row.toString(), missingFiles, esMissing, icebergMissing);
  }

  /** Logs progress every PROGRESS_INTERVAL completed datasets (called from worker threads). */
  private static void logProgress(int done, int total, long start) {
    if (done % PROGRESS_INTERVAL == 0 || done == total) {
      long elapsedMs = System.currentTimeMillis() - start;
      long etaSec = (elapsedMs * (total - done) / done) / 1000;
      log.info(
          "Report progress: {}/{} datasets ({}%), elapsed {} s, ETA {} s",
          done, total, done * 100 / total, elapsedMs / 1000, etaSec);
    }
  }

  /** Waits for a row, rethrowing a worker's failure as an IOException. */
  private static DatasetRow getRow(Future<DatasetRow> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while building the report", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException ioe) {
        throw ioe;
      }
      throw new IOException("Failed to build a report row", e.getCause());
    }
  }

  /**
   * Appends one CSV cell per column of the metrics file. If the file doesn't exist every cell is
   * NOT_AVAILABLE; if it exists but a metric is missing (or unreadable) that cell is left blank.
   *
   * @return true if the metrics file exists
   */
  private static boolean appendMetrics(
      FileSystem fileSystem, String metricsPath, MetricsFile metricsFile, StringBuilder row)
      throws IOException {

    if (!fileSystem.exists(new Path(metricsPath))) {
      for (int i = 0; i < metricsFile.columns().size(); i++) {
        row.append(',').append(NOT_AVAILABLE);
      }
      log.debug("{} not available", metricsPath);
      return false;
    }

    // returns an empty map (and logs) if the file can't be read
    Map<String, Long> metrics = MetricsUtil.readMetricsYaml(fileSystem, metricsPath);
    if (metrics.isEmpty()) {
      log.warn("No metrics read from {}", metricsPath);
    }
    for (String column : metricsFile.columns()) {
      Long value = metrics.get(column);
      if (value == null && !metrics.isEmpty()) {
        log.debug("{} has no metric {}", metricsPath, column);
      }
      row.append(',').append(value != null ? value.toString() : "");
    }
    return true;
  }
}
