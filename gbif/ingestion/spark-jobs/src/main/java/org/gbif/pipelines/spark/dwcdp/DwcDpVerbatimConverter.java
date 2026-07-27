package org.gbif.pipelines.spark.dwcdp;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.builder.extension.AssertionExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.HumboldtExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageResource;
import org.gbif.pipelines.spark.util.PathUtil;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Converts DwC-DP Parquet files (written by DataPackageConversionPipeline) into verbatim.avro.
 *
 * <p>Routing:
 *
 * <ul>
 *   <li>{@code containsEvents} and {@code event} table present → {@link EventCoreBuilder}
 *   <li>{@code containsOccurrences} and {@code occurrence} table present → {@link
 *       OccurrenceCoreBuilder}
 *   <li>Otherwise → empty verbatim (logged as warning)
 * </ul>
 *
 * <p>The production {@link TableLoader} is constructed here as a lambda over {@code
 * spark.read().parquet()} and the resolved Parquet paths from the {@link DataPackage} descriptor.
 * All extension building, organism denormalization, and join logic is delegated to the {@code
 * builder} sub-package; this class owns only orchestration, Avro output, and metrics.
 */
@Slf4j
public class DwcDpVerbatimConverter {

  // Core row type URIs
  public static final String CORE_ROW_TYPE_EVENT = DwcTerm.Event.qualifiedName();
  public static final String CORE_ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  // Extension row type for occurrences attached to an event core
  public static final String ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  // Extension row type URIs — forwarded from builder classes for callers that import this class
  public static final String ROW_TYPE_MULTIMEDIA =
      org.gbif.pipelines.spark.dwcdp.builder.extension.MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA;
  public static final String ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT =
      AssertionExtensionBuilder.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT;
  public static final String ROW_TYPE_HUMBOLDT = HumboldtExtensionBuilder.ROW_TYPE_HUMBOLDT;

  private static final org.apache.avro.Schema EXTENDED_RECORD_SCHEMA = loadExtendedRecordSchema();
  static final String AVRO_EXTENDED_RECORD_AVSC = "avro/extended-record.avsc";

  private DwcDpVerbatimConverter() {}

  public record VerbatimConversionMetrics(
      long erCount, long occurrenceCount, long eventCount, long largestFileCount) {}

  public static VerbatimConversionMetrics convert(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      String datasetId,
      int attempt,
      boolean containsEvents,
      boolean containsOccurrences)
      throws IOException {

    ThreadContext.put("datasetKey", datasetId);
    ThreadContext.put("attempt", String.valueOf(attempt));

    long start = System.currentTimeMillis();
    log.info(
        "Starting DwcDpVerbatimConverter for dataset {} attempt {}, containsEvents={}, containsOccurrences={}",
        datasetId,
        attempt,
        containsEvents,
        containsOccurrences);

    String parquetBasePath =
        PathUtil.interpretedAttemptPath(config.getOutputPath(), datasetId, attempt);
    String verbatimOutputPath =
        PathUtil.interpretedAttemptPath(config.getInputPath(), datasetId, attempt)
            + "/verbatim.avro";

    DataPackage dataPackage =
        DataPackageDescriptorReader.read(fileSystem, parquetBasePath + "/datapackage.json");

    // Production TableLoader: resolves table names to Parquet paths via the DataPackage
    // descriptor, returning Optional.empty() for tables not listed in the package.
    TableLoader loader =
        tableName ->
            dataPackage
                .findResource(tableName)
                .map(r -> spark.read().parquet(parquetBasePath + "/" + r.getPath()));

    Dataset<ExtendedRecord> records;

    if (containsEvents && dataPackage.findResource("event").isPresent()) {
      log.info("Building event-core ExtendedRecords");
      records = EventCoreBuilder.build(spark, loader);
    } else if (containsOccurrences && dataPackage.findResource("occurrence").isPresent()) {
      log.info("Building occurrence-core ExtendedRecords");
      records = OccurrenceCoreBuilder.build(spark, loader);
    } else {
      log.warn(
          "Dataset {} has no event or occurrence table in datapackage.json; writing empty verbatim",
          datasetId);
      records = spark.emptyDataset(Encoders.bean(ExtendedRecord.class));
    }

    String tempOutputPath = verbatimOutputPath + ".parts";
    records
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .option("avroSchema", EXTENDED_RECORD_SCHEMA.toString())
        .save(tempOutputPath);

    mergeToSingleFile(fileSystem, tempOutputPath, verbatimOutputPath);

    VerbatimConversionMetrics metrics =
        writeMetrics(spark, dataPackage, parquetBasePath, fileSystem, datasetId);

    log.info(
        "DwcDpVerbatimConverter completed for dataset {} attempt {} in {}ms, metrics: {}",
        datasetId,
        attempt,
        System.currentTimeMillis() - start,
        metrics);

    return metrics;
  }

  /**
   * Convenience method for tests and callers that have a {@link DataPackage} descriptor and a base
   * path but no pre-built {@link TableLoader}. Constructs a loader that reads Parquet files via
   * {@code spark.read().parquet()}, then delegates to {@link EventCoreBuilder#build}.
   */
  static Dataset<ExtendedRecord> buildEventCoreDataset(
      SparkSession spark, DataPackage dataPackage, String basePath) {
    TableLoader loader =
        tableName ->
            dataPackage
                .findResource(tableName)
                .map(r -> spark.read().parquet(basePath + "/" + r.getPath()));
    return EventCoreBuilder.build(spark, loader);
  }

  /**
   * Convenience method for tests and callers that have a {@link DataPackage} descriptor and a base
   * path but no pre-built {@link TableLoader}. Constructs a loader that reads Parquet files via
   * {@code spark.read().parquet()}, then delegates to {@link OccurrenceCoreBuilder#build}.
   */
  static Dataset<ExtendedRecord> buildOccurrenceCoreDataset(
      SparkSession spark, DataPackage dataPackage, String basePath) {
    TableLoader loader =
        tableName ->
            dataPackage
                .findResource(tableName)
                .map(r -> spark.read().parquet(basePath + "/" + r.getPath()));
    return OccurrenceCoreBuilder.build(spark, loader);
  }

  static VerbatimConversionMetrics writeMetrics(
      SparkSession spark,
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId) {

    long occurrenceCount =
        dataPackage
            .findResource("occurrence")
            .map(r -> countRows(spark, datasetBasePath, r))
            .orElse(0L);

    long eventCount =
        dataPackage.findResource("event").map(r -> countRows(spark, datasetBasePath, r)).orElse(0L);

    long largestFileCount =
        dataPackage.getResources().stream()
            .mapToLong(r -> countRows(spark, datasetBasePath, r))
            .max()
            .orElse(0L);

    Map<String, Long> metrics =
        Map.of(
            Metrics.ARCHIVE_TO_ER_COUNT, 0L,
            Metrics.ARCHIVE_TO_OCC_COUNT, occurrenceCount,
            Metrics.EVENT_CORE_RECORDS_COUNT, eventCount,
            Metrics.ARCHIVE_TO_LARGEST_FILE_COUNT, largestFileCount);

    String metricsPath = datasetBasePath + "/" + Pipeline.ARCHIVE_TO_VERBATIM + ".yml";
    log.info("Writing verbatim metrics for dataset {}: {}", datasetId, metrics);
    MetricsUtil.writeMetricsYaml(fileSystem, metrics, metricsPath);

    return new VerbatimConversionMetrics(0L, occurrenceCount, eventCount, largestFileCount);
  }

  static void mergeToSingleFile(FileSystem fileSystem, String tempPath, String targetPath)
      throws IOException {
    org.apache.hadoop.fs.Path temp = new org.apache.hadoop.fs.Path(tempPath);
    org.apache.hadoop.fs.Path target = new org.apache.hadoop.fs.Path(targetPath);

    if (fileSystem.exists(target)) {
      fileSystem.delete(target, true);
    }

    org.apache.hadoop.fs.FileStatus partFile =
        Arrays.stream(fileSystem.listStatus(temp))
            .filter(s -> s.getPath().getName().endsWith(".avro"))
            .findFirst()
            .orElseThrow(
                () -> new IOException("No .avro part file found in temp directory: " + tempPath));

    if (!fileSystem.rename(partFile.getPath(), target)) {
      throw new IOException(
          "Failed to rename avro part file " + partFile.getPath() + " to " + target);
    }
    fileSystem.delete(temp, true);

    log.info("Merged single avro part file to {}", targetPath);
  }

  private static long countRows(
      SparkSession spark, String datasetBasePath, DataPackageResource resource) {
    return spark.read().parquet(datasetBasePath + "/" + resource.getPath()).count();
  }

  static String extendedRecordSchemaJson() {
    return EXTENDED_RECORD_SCHEMA.toString();
  }

  /** Resolves a column name to a qualified term URI. Delegates to {@link TermResolver#resolve}. */
  static String resolveTermUri(String columnName) {
    return TermResolver.resolve(columnName);
  }

  private static org.apache.avro.Schema loadExtendedRecordSchema() {
    try (var stream =
        DwcDpVerbatimConverter.class
            .getClassLoader()
            .getResourceAsStream(AVRO_EXTENDED_RECORD_AVSC)) {
      if (stream == null) {
        throw new IllegalStateException(
            "extended-record.avsc not found on classpath — copy it to src/main/resources/");
      }
      return new org.apache.avro.Schema.Parser().parse(stream);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load extended-record.avsc", e);
    }
  }
}
