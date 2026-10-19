package org.gbif.pipelines.spark.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EXTENSION_PREFIX;
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
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
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
 * written as one row of a gzipped CSV ({@value #REPORT_EXTENSION}) to HDFS (default directory
 * {@code /tmp}).
 *
 * <p>Each row also has the dataset's {@code title}, {@code type} and {@code sub_type} (after the
 * attempt column), joined on dataset key from the registry's dataset export TSV. They are blank for
 * a dataset missing from the export.
 *
 * <p>The {@code extension_*} counts in {@value #ARCHIVE_TO_VERBATIM_FILENAME} (records per DwC-A
 * extension, e.g. {@code extension_rs_Multimedia}) vary by dataset, so the report has one column
 * for every extension found in any dataset, after the fixed metric columns. A dataset without that
 * extension gets 0, and NOT_AVAILABLE when it has no {@value #ARCHIVE_TO_VERBATIM_FILENAME}.
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
  static final String ARCHIVE_TO_VERBATIM_FILENAME = "archive-to-verbatim.yml";
  static final String ES_COUNT_COLUMN = "ElasticOccurrenceCount";
  static final String ICEBERG_COUNT_COLUMN = "IcebergOccurrenceCount";
  static final String ES_FACET_PATH =
      "/occurrence/search?limit=0&facet=datasetKey&facetLimit=120000";
  static final String DATASET_EXPORT_URL = "/dataset/search/export";
  /** The report columns taken from the dataset export, in order, and the export column names. */
  static final List<String> DATASET_METADATA_COLUMNS = List.of("title", "type", "sub_type");

  static final String DATASET_KEY_COLUMN = "dataset_key";

  /** The report's columns from the dataset export, in {@link #DATASET_METADATA_COLUMNS} order. */
  record DatasetMetadata(String title, String type, String subType) {

    /** The CSV cells, each with a leading comma; blank cells if there is no metadata. */
    static String cells(DatasetMetadata metadata) {
      if (metadata == null) {
        return ",".repeat(DATASET_METADATA_COLUMNS.size());
      }
      return ","
          + csvCell(metadata.title())
          + ","
          + csvCell(metadata.type())
          + ","
          + csvCell(metadata.subType());
    }
  }

  /** The report is always a gzipped CSV; any other extension given in the filename is replaced. */
  static final String REPORT_EXTENSION = ".csv.gz";

  /**
   * Suffix format for backing up an existing report. Colons aren't allowed in HDFS path names, so
   * the time uses dashes.
   */
  static final DateTimeFormatter BACKUP_SUFFIX_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").withZone(ZoneId.systemDefault());

  /** How often (in datasets) to log report-writing progress. */
  static final int PROGRESS_INTERVAL = 500;

  /**
   * The CSV cells for one dataset, plus what was missing, so counts can be tallied in order. The
   * extension columns aren't known until every dataset is read, so the row is assembled later from
   * the fixed metric cells, the extension counts and the ES/Iceberg count cells.
   *
   * @param metricCells datasetUUID, attempt, dataset metadata and the fixed metric cells, comma
   *     separated
   * @param extensionCounts the extension_* metrics, or null if there is no archive-to-verbatim.yml
   * @param countCells the ES/Iceberg count cells, each with a leading comma (empty if disabled)
   */
  record DatasetRow(
      String metricCells,
      Map<String, Long> extensionCounts,
      String countCells,
      List<String> missingFiles,
      boolean metadataMissing,
      boolean esMissing,
      boolean icebergMissing) {

    /** The full CSV row, with a cell for each of the given extension columns. */
    String row(SortedSet<String> extensionColumns) {
      StringBuilder row = new StringBuilder(metricCells);
      for (String extension : extensionColumns) {
        row.append(',');
        if (extensionCounts == null) {
          row.append(NOT_AVAILABLE);
        } else {
          row.append(extensionCounts.getOrDefault(extension, 0L));
        }
      }
      return row.append(countCells).toString();
    }
  }

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
              ARCHIVE_TO_VERBATIM_FILENAME,
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
        description =
            "Report filename. Its extension is replaced with .csv.gz. Defaults to"
                + " metrics-report-<timestamp>.csv.gz")
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

    log.info("Step 1/4: fetching dataset metadata");
    Map<String, DatasetMetadata> datasetMetadata =
        fetchDatasetMetadata(
            config.getGbifApi().getWsUrl() + DATASET_EXPORT_URL, config.getGbifApi());

    Map<String, Long> esCounts = null;
    if (args.esCounts) {
      log.info("Step 2/4: fetching Elasticsearch counts");
      esCounts = fetchEsCounts(config);
    } else {
      log.info("Step 2/4: Elasticsearch counts disabled (--esCounts=true to enable)");
    }

    Map<String, Long> icebergCounts = null;
    if (args.icebergCounts) {
      log.info("Step 3/4: fetching occurrence table counts");
      icebergCounts = fetchIcebergCounts(config, args.master);
    } else {
      log.info("Step 3/4: occurrence table counts disabled (--icebergCounts=true to enable)");
    }

    Configuration hadoopConf = new Configuration();
    hadoopConf.addResource(new Path(config.getHdfsSiteConfig()));
    hadoopConf.addResource(new Path(config.getCoreSiteConfig()));
    FileSystem fileSystem = FileSystem.get(hadoopConf);

    String filename =
        args.outputFilename != null
            ? args.outputFilename
            : "metrics-report-" + System.currentTimeMillis() + ".csv.gz";
    Path reportPath = new Path(args.outputDir, reportFilename(filename));

    log.info("Step 4/4: scanning datasets and writing report to {}", reportPath);
    int rows =
        writeReport(
            fileSystem, config, reportPath, datasetMetadata, esCounts, icebergCounts, args.threads);
    log.info(
        "Finished: wrote metrics for {} datasets to {} in {} s",
        rows,
        reportPath,
        (System.currentTimeMillis() - start) / 1000);
  }

  /**
   * Downloads the dataset export TSV and returns dataset key to its metadata.
   *
   * @throws IOException if the request fails or the TSV isn't the expected shape
   */
  static Map<String, DatasetMetadata> fetchDatasetMetadata(String url, WsConfig api)
      throws IOException, InterruptedException {

    URI uri = URI.create(url);
    log.info("Fetching dataset metadata from {}", uri);
    long start = System.currentTimeMillis();

    // the export is large (~50 MB), so allow it plenty of time
    HttpRequest request =
        HttpRequest.newBuilder(uri)
            .timeout(Duration.ofSeconds(Math.max(api.getTimeoutSec(), 600)))
            .GET()
            .build();

    HttpResponse<Stream<String>> response =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(api.getTimeoutSec()))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build()
            .send(request, HttpResponse.BodyHandlers.ofLines());

    try (Stream<String> lines = response.body()) {
      if (response.statusCode() != 200) {
        throw new IOException(
            "Dataset export returned HTTP " + response.statusCode() + " for " + uri);
      }
      Map<String, DatasetMetadata> metadata = parseDatasetExport(lines);
      log.info(
          "Fetched metadata for {} datasets in {} s",
          metadata.size(),
          (System.currentTimeMillis() - start) / 1000);
      return metadata;
    }
  }

  /**
   * Parses the dataset export TSV, finding the columns by their header names. Fields containing
   * quotes are quoted CSV style, with embedded quotes doubled.
   */
  static Map<String, DatasetMetadata> parseDatasetExport(Stream<String> lines) throws IOException {
    java.util.Iterator<String> it = lines.iterator();
    if (!it.hasNext()) {
      throw new IOException("Dataset export is empty");
    }
    List<String> header = List.of(it.next().split("\t", -1));
    int keyIndex = columnIndex(header, DATASET_KEY_COLUMN);
    int titleIndex = columnIndex(header, DATASET_METADATA_COLUMNS.get(0));
    int typeIndex = columnIndex(header, DATASET_METADATA_COLUMNS.get(1));
    int subTypeIndex = columnIndex(header, DATASET_METADATA_COLUMNS.get(2));

    Map<String, DatasetMetadata> metadata = new HashMap<>();
    int skipped = 0;
    while (it.hasNext()) {
      String[] fields = it.next().split("\t", -1);
      if (fields.length != header.size()) {
        skipped++;
        continue;
      }
      metadata.put(
          unquoteTsv(fields[keyIndex]),
          new DatasetMetadata(
              unquoteTsv(fields[titleIndex]),
              unquoteTsv(fields[typeIndex]),
              unquoteTsv(fields[subTypeIndex])));
    }
    if (skipped > 0) {
      log.warn("Skipped {} dataset export lines without {} fields", skipped, header.size());
    }
    return metadata;
  }

  private static int columnIndex(List<String> header, String column) throws IOException {
    int index = header.indexOf(column);
    if (index < 0) {
      throw new IOException("Dataset export has no " + column + " column: " + header);
    }
    return index;
  }

  /** Removes the CSV-style quoting the export uses for fields containing quotes. */
  static String unquoteTsv(String field) {
    if (field.length() >= 2 && field.startsWith("\"") && field.endsWith("\"")) {
      return field.substring(1, field.length() - 1).replace("\"\"", "\"");
    }
    return field;
  }

  /** Quotes a CSV cell if it contains a comma, quote or line break. */
  static String csvCell(String value) {
    if (value == null) {
      return "";
    }
    if (value.contains(",")
        || value.contains("\"")
        || value.contains("\n")
        || value.contains("\r")) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
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
   * @param datasetMetadata datasetKey to export metadata (title, type, sub_type); null or a missing
   *     dataset leaves those cells blank
   * @param esCounts datasetKey to Elasticsearch count, or null to omit the column
   * @param icebergCounts datasetKey to occurrence table count, or null to omit the column
   * @param threads number of threads reading the metrics files
   * @return the number of dataset rows written
   */
  public static int writeReport(
      FileSystem fileSystem,
      PipelinesConfig config,
      Path reportPath,
      Map<String, DatasetMetadata> datasetMetadata,
      Map<String, Long> esCounts,
      Map<String, Long> icebergCounts,
      int threads)
      throws IOException {

    Map<String, DatasetMetadata> metadata = datasetMetadata != null ? datasetMetadata : Map.of();
    reportPath = new Path(reportPath.getParent(), reportFilename(reportPath.getName()));

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
    int metadataMissing = 0;
    int esMissing = 0;
    int icebergMissing = 0;

    // Build each dataset's row on a thread pool (the work is HDFS round trips), keeping them in
    // submission order so the report stays sorted by dataset UUID. All rows are read before
    // writing, as the extension columns are the union over every dataset. Counters are tallied on
    // this thread only.
    List<DatasetRow> datasetRows = new java.util.ArrayList<>(total);
    SortedSet<String> extensionColumns = new TreeSet<>();
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
                      buildRow(
                          fileSystem,
                          config,
                          datasetId,
                          attempt,
                          metadata,
                          esCounts,
                          icebergCounts);
                  logProgress(completed.incrementAndGet(), total, start);
                  return datasetRow;
                }));
      }

      for (Future<DatasetRow> future : futures) {
        DatasetRow datasetRow = getRow(future);
        for (String file : datasetRow.missingFiles()) {
          missingCounts.merge(file, 1, Integer::sum);
        }
        if (datasetRow.metadataMissing()) {
          metadataMissing++;
        }
        if (datasetRow.esMissing()) {
          esMissing++;
        }
        if (datasetRow.icebergMissing()) {
          icebergMissing++;
        }
        if (datasetRow.extensionCounts() != null) {
          extensionColumns.addAll(datasetRow.extensionCounts().keySet());
        }
        datasetRows.add(datasetRow);
      }
    } finally {
      executor.shutdownNow();
    }
    log.info("Found {} extension columns: {}", extensionColumns.size(), extensionColumns);

    backupExistingReport(fileSystem, reportPath);

    int rows = 0;
    try (FSDataOutputStream out = fileSystem.create(reportPath, false);
        BufferedWriter writer =
            new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(out), UTF_8))) {

      StringBuilder header = new StringBuilder("datasetUUID,attempt");
      DATASET_METADATA_COLUMNS.forEach(column -> header.append(',').append(column));
      for (MetricsFile metricsFile : METRICS_FILES) {
        for (String column : metricsFile.columns()) {
          header.append(',').append(column);
        }
      }
      extensionColumns.forEach(extension -> header.append(',').append(extension));
      if (esCounts != null) {
        header.append(',').append(ES_COUNT_COLUMN);
      }
      if (icebergCounts != null) {
        header.append(',').append(ICEBERG_COUNT_COLUMN);
      }
      writer.write(header.toString());
      writer.newLine();

      for (DatasetRow datasetRow : datasetRows) {
        String row = datasetRow.row(extensionColumns);
        writer.write(row);
        writer.newLine();
        rows++;
        log.debug("Row {}/{}: {}", rows, total, row);
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
    log.info(
        "  {}: {} datasets not in the dataset export (left blank)",
        DATASET_METADATA_COLUMNS,
        metadataMissing);
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

  /**
   * The report filename with its extension (if any) replaced by {@value #REPORT_EXTENSION}, e.g.
   * {@code report}, {@code report.csv} and {@code report.csv.gz} all become {@code report.csv.gz}.
   */
  static String reportFilename(String filename) {
    String base = filename;
    if (base.toLowerCase().endsWith(REPORT_EXTENSION)) {
      base = base.substring(0, base.length() - REPORT_EXTENSION.length());
    } else if (base.lastIndexOf('.') > 0) {
      base = base.substring(0, base.lastIndexOf('.'));
    }
    return base + REPORT_EXTENSION;
  }

  /**
   * Renames an existing report {@code <name>.csv.gz} to {@code
   * <name>-<yyyy-MM-dd_HH-mm-ss>.csv.gz}, using the time it was last modified, so a new report
   * doesn't overwrite it.
   *
   * @throws IOException if the rename fails (e.g. the backup name is already taken)
   */
  static void backupExistingReport(FileSystem fileSystem, Path reportPath) throws IOException {
    if (!fileSystem.exists(reportPath)) {
      return;
    }
    long modified = fileSystem.getFileStatus(reportPath).getModificationTime();
    String name = reportFilename(reportPath.getName());
    Path backupPath =
        new Path(
            reportPath.getParent(),
            name.substring(0, name.length() - REPORT_EXTENSION.length())
                + "-"
                + BACKUP_SUFFIX_FORMAT.format(Instant.ofEpochMilli(modified))
                + REPORT_EXTENSION);
    if (fileSystem.exists(backupPath) || !fileSystem.rename(reportPath, backupPath)) {
      throw new IOException(
          "Failed to back up existing report " + reportPath + " to " + backupPath);
    }
    log.info("Existing report {} renamed to {}", reportPath, backupPath);
  }

  /** Builds the CSV row for one dataset. Thread safe: only reads from HDFS and the count maps. */
  private static DatasetRow buildRow(
      FileSystem fileSystem,
      PipelinesConfig config,
      String datasetId,
      int attempt,
      Map<String, DatasetMetadata> datasetMetadata,
      Map<String, Long> esCounts,
      Map<String, Long> icebergCounts)
      throws IOException {

    String attemptDir =
        String.join("/", config.getOutputPath(), datasetId, String.valueOf(attempt));

    DatasetMetadata metadata = datasetMetadata.get(datasetId);
    StringBuilder row =
        new StringBuilder(datasetId)
            .append(',')
            .append(attempt)
            .append(DatasetMetadata.cells(metadata));
    List<String> missingFiles = new java.util.ArrayList<>();
    Map<String, Long> extensionCounts = null;
    for (MetricsFile metricsFile : METRICS_FILES) {
      Map<String, Long> metrics =
          appendMetrics(fileSystem, attemptDir + "/" + metricsFile.filename(), metricsFile, row);
      if (metrics == null) {
        missingFiles.add(metricsFile.filename());
      } else if (ARCHIVE_TO_VERBATIM_FILENAME.equals(metricsFile.filename())) {
        extensionCounts = extensionCounts(metrics);
      }
    }

    // a dataset absent from the facet / group-by has no records there
    StringBuilder countCells = new StringBuilder();
    boolean esMissing = false;
    if (esCounts != null) {
      esMissing = !esCounts.containsKey(datasetId);
      countCells.append(',').append(esCounts.getOrDefault(datasetId, 0L));
    }
    boolean icebergMissing = false;
    if (icebergCounts != null) {
      icebergMissing = !icebergCounts.containsKey(datasetId);
      countCells.append(',').append(icebergCounts.getOrDefault(datasetId, 0L));
    }
    return new DatasetRow(
        row.toString(),
        extensionCounts,
        countCells.toString(),
        missingFiles,
        metadata == null,
        esMissing,
        icebergMissing);
  }

  /** The extension_* metrics (records per DwC-A extension) from archive-to-verbatim metrics. */
  static Map<String, Long> extensionCounts(Map<String, Long> metrics) {
    Map<String, Long> extensions = new HashMap<>();
    metrics.forEach(
        (key, value) -> {
          if (key.startsWith(EXTENSION_PREFIX)) {
            extensions.put(key, value);
          }
        });
    return extensions;
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
   * @return the metrics read (empty if unreadable), or null if the metrics file doesn't exist
   */
  private static Map<String, Long> appendMetrics(
      FileSystem fileSystem, String metricsPath, MetricsFile metricsFile, StringBuilder row)
      throws IOException {

    if (!fileSystem.exists(new Path(metricsPath))) {
      for (int i = 0; i < metricsFile.columns().size(); i++) {
        row.append(',').append(NOT_AVAILABLE);
      }
      log.debug("{} not available", metricsPath);
      return null;
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
    return metrics;
  }
}
