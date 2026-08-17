package org.gbif.pipelines.spark.dwcdp;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.mapping.config.AssertionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventDwcaMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.config.HumboldtMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.config.MultimediaMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.config.OccurrenceDwcaMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
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
 *   <li>{@code containsEvents} and {@code event} table present → canonical Event mapping plan
 *   <li>{@code containsOccurrences} and {@code occurrence} table present → canonical Occurrence
 *       mapping plan
 *   <li>Otherwise → empty verbatim (logged as warning)
 * </ul>
 *
 * <p>The production {@link TableLoader} is constructed here as a lambda over {@code
 * spark.read().parquet()} and the resolved Parquet paths from the {@link DataPackage} descriptor.
 * Mapping compilation and Spark execution are delegated to {@link DwcDpMappingEngine}; this class
 * owns only routing, Parquet loading, Avro output, and metrics.
 */
@Slf4j
public class DwcDpVerbatimConverter {

  // Core row type URIs
  public static final String CORE_ROW_TYPE_EVENT = DwcTerm.Event.qualifiedName();
  public static final String CORE_ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  // Extension row type for occurrences attached to an event core
  public static final String ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  // Extension row type URIs — owned by the declarative mapping configuration.
  public static final String ROW_TYPE_MULTIMEDIA = MultimediaMapping.ROW_TYPE_MULTIMEDIA;
  public static final String ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT =
      AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT;
  public static final String ROW_TYPE_HUMBOLDT = HumboldtMapping.ROW_TYPE_HUMBOLDT;

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
    DwcDpMappingEngine mappingEngine = DwcDpMappingEngine.currentSchema();

    if (containsEvents && dataPackage.findResource("event").isPresent()) {
      log.info("Building event-core ExtendedRecords with declarative mapping engine");
      records =
          mappingEngine.execute(loader, EventDwcaMapping.current(mappingEngine.schemaGraph()));
    } else if (containsOccurrences && dataPackage.findResource("occurrence").isPresent()) {
      log.info("Building occurrence-core ExtendedRecords with declarative mapping engine");
      records =
          mappingEngine.execute(loader, OccurrenceDwcaMapping.current(mappingEngine.schemaGraph()));
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
        writeMetrics(
            spark,
            dataPackage,
            parquetBasePath,
            fileSystem,
            datasetId,
            Optional.of(records));

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
   * path but no pre-built {@link TableLoader}. Executes the canonical Event mapping plan.
   */
  static Dataset<ExtendedRecord> buildEventCoreDataset(
      SparkSession spark, DataPackage dataPackage, String basePath) {
    return buildEventCoreDataset(spark, parquetTableLoader(spark, dataPackage, basePath));
  }

  /** Executes the canonical Event mapping plan using an already constructed table loader. */
  static Dataset<ExtendedRecord> buildEventCoreDataset(SparkSession spark, TableLoader loader) {
    DwcDpMappingEngine mappingEngine = DwcDpMappingEngine.currentSchema();
    return mappingEngine.execute(loader, EventDwcaMapping.current(mappingEngine.schemaGraph()));
  }

  /**
   * Convenience method for tests and callers that have a {@link DataPackage} descriptor and a base
   * path but no pre-built {@link TableLoader}. Executes the canonical Occurrence mapping plan.
   */
  static Dataset<ExtendedRecord> buildOccurrenceCoreDataset(
      SparkSession spark, DataPackage dataPackage, String basePath) {
    return buildOccurrenceCoreDataset(spark, parquetTableLoader(spark, dataPackage, basePath));
  }

  /** Executes the canonical Occurrence mapping plan using an already constructed table loader. */
  static Dataset<ExtendedRecord> buildOccurrenceCoreDataset(SparkSession spark, TableLoader loader) {
    DwcDpMappingEngine mappingEngine = DwcDpMappingEngine.currentSchema();
    return mappingEngine.execute(loader, OccurrenceDwcaMapping.current(mappingEngine.schemaGraph()));
  }

  private static TableLoader parquetTableLoader(
      SparkSession spark, DataPackage dataPackage, String basePath) {
    return tableName ->
        dataPackage
            .findResource(tableName)
            .map(r -> spark.read().parquet(basePath + "/" + r.getPath()));
  }

  static VerbatimConversionMetrics writeMetrics(
      SparkSession spark,
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId) {
    return writeMetrics(
        spark, dataPackage, datasetBasePath, fileSystem, datasetId, Optional.empty());
  }

  static VerbatimConversionMetrics writeMetrics(
      SparkSession spark,
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId,
      Optional<Dataset<ExtendedRecord>> verbatimDataset) {

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
    writeConversionReport(
        spark, dataPackage, datasetBasePath, fileSystem, datasetId, verbatimDataset);

    return new VerbatimConversionMetrics(0L, occurrenceCount, eventCount, largestFileCount);
  }

  private static void writeConversionReport(
      SparkSession spark,
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId,
      Optional<Dataset<ExtendedRecord>> verbatimDataset) {
    List<String> lines = new ArrayList<>();
    lines.add("DwC-DP conversion report: " + datasetId);
    lines.add("");
    lines.add("source tables (raw row counts):");

    dataPackage.getResources().stream()
        .sorted(Comparator.comparing(DataPackageResource::getName))
        .forEach(
            resource ->
                lines.add(
                    "  "
                        + resource.getName()
                        + ": "
                        + countRows(spark, datasetBasePath, resource)));

    appendMaterialFunnel(spark, dataPackage, datasetBasePath, lines);

    lines.add("");
    lines.add("output extensions (rows actually written):");
    if (verbatimDataset.isPresent()) {
      Dataset<ExtendedRecord> records = verbatimDataset.get();
      long coreRecords = records.count();
      lines.add("  core records written: " + coreRecords);

      Dataset<Row> extensionStats =
          records
              .toDF()
              .selectExpr("explode(extensions) as (rowType, rows)")
              .groupBy("rowType")
              .agg(
                  functions.sum(functions.size(functions.col("rows"))).alias("rows"),
                  functions.count(functions.lit(1)).alias("records"))
              .orderBy("rowType");

      for (Row row : extensionStats.collectAsList()) {
        lines.add(
            "  "
                + row.getAs("rowType")
                + ": rows="
                + row.getAs("rows")
                + ", records-with-this-ext="
                + row.getAs("records"));
      }
    } else {
      lines.add("  core records written: 0");
      lines.add("  (output dataset not supplied)");
    }

    org.apache.hadoop.fs.Path reportPath =
        new org.apache.hadoop.fs.Path(datasetBasePath + "/conversion-report.txt");
    try (BufferedWriter writer =
        new BufferedWriter(
            new OutputStreamWriter(fileSystem.create(reportPath, true), StandardCharsets.UTF_8))) {
      for (String line : lines) {
        writer.write(line);
        writer.newLine();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to write DwC-DP conversion report", e);
    }
  }

  private static void appendMaterialFunnel(
      SparkSession spark, DataPackage dataPackage, String datasetBasePath, List<String> lines) {
    Optional<DataPackageResource> materialResource = dataPackage.findResource("material");
    if (materialResource.isEmpty()) {
      return;
    }

    Dataset<Row> material =
        spark.read().parquet(datasetBasePath + "/" + materialResource.get().getPath());
    long total = material.count();
    boolean hasEvidence = Arrays.asList(material.columns()).contains("evidenceForOccurrenceID");

    Dataset<Row> withEvidence =
        hasEvidence
            ? material.filter(
                functions.col("evidenceForOccurrenceID").isNotNull()
                    .and(functions.length(functions.trim(functions.col("evidenceForOccurrenceID"))).gt(0)))
            : material.limit(0);
    long withEvidenceCount = withEvidence.count();
    long withoutEvidenceCount = total - withEvidenceCount;

    long enriched = 0L;
    long ambiguous = 0L;
    if (hasEvidence && dataPackage.findResource("occurrence").isPresent()) {
      DataPackageResource occurrenceResource = dataPackage.findResource("occurrence").orElseThrow();
      Dataset<Row> occurrence =
          spark.read().parquet(datasetBasePath + "/" + occurrenceResource.getPath());
      if (Arrays.asList(occurrence.columns()).contains("occurrenceID")) {
        Dataset<Row> localEvidence =
            withEvidence
                .join(
                    occurrence.select(functions.col("occurrenceID").alias("__local_occurrence_id")),
                    withEvidence
                        .col("evidenceForOccurrenceID")
                        .equalTo(functions.col("__local_occurrence_id")),
                    "inner")
                .drop("__local_occurrence_id");
        Dataset<Row> evidenceCounts =
            localEvidence.groupBy("evidenceForOccurrenceID").count();
        enriched = evidenceCounts.filter(functions.col("count").equalTo(1)).count();
        ambiguous =
            evidenceCounts
                .filter(functions.col("count").gt(1))
                .agg(functions.coalesce(functions.sum("count"), functions.lit(0L)).alias("n"))
                .first()
                .getLong(0);
      }
    }

    long virtual = 0L; // Virtual material occurrences are intentionally paused.
    long unresolvedWithoutEvidence = withoutEvidenceCount - virtual;

    lines.add("");
    lines.add("material funnel:");
    lines.add("  material rows (total): " + total);
    lines.add("  without evidence: " + withoutEvidenceCount);
    lines.add("    -> became virtual occurrence: " + virtual);
    lines.add("    -> unresolved, DROPPED: " + unresolvedWithoutEvidence);
    lines.add("  with evidence: " + withEvidenceCount);
    lines.add("    -> enriched real occurrence: " + enriched);
    lines.add("    -> ambiguous, DROPPED: " + ambiguous);
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

  /** Resolves known term names for converter-level tests; unknown extension keys remain raw. */
  static String resolveTermUri(String columnName) {
    Term term = TermFactory.instance().findTerm(columnName);
    return term != null && !(term instanceof UnknownTerm) ? term.qualifiedName() : columnName;
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
