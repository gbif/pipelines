package org.gbif.pipelines.spark.dwcdp;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
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
import org.apache.spark.storage.StorageLevel;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingExecutionOutput;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingBranchExecutionMetrics;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationExecutionMetrics;
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
  static final String INGEST_PLAN_COMPACT = "dwcdp-ingest-plan-compact.txt";
  static final String INGEST_PLAN_DETAILED = "dwcdp-ingest-plan-detailed.txt";

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
    MappingExecutionOutput mappingExecution = null;
    List<MappingBranchExecutionMetrics> branchMetrics = List.of();
    DwcDpMappingEngine mappingEngine = DwcDpMappingEngine.currentSchema();
    MappingPlan ingestPlan = null;

    if (containsEvents && dataPackage.findResource("event").isPresent()) {
      log.info("Building event-core ExtendedRecords with declarative mapping engine");
      ingestPlan = EventDwcaMapping.current(mappingEngine.schemaGraph());
      mappingExecution = mappingEngine.executeWithMetrics(loader, ingestPlan, dataPackage);
      records = mappingExecution.records();
      branchMetrics = mappingExecution.branchMetrics();
    } else if (containsOccurrences && dataPackage.findResource("occurrence").isPresent()) {
      log.info("Building occurrence-core ExtendedRecords with declarative mapping engine");
      ingestPlan = OccurrenceDwcaMapping.current(mappingEngine.schemaGraph());
      mappingExecution = mappingEngine.executeWithMetrics(loader, ingestPlan, dataPackage);
      records = mappingExecution.records();
      branchMetrics = mappingExecution.branchMetrics();
    } else {
      log.warn(
          "Dataset {} has no event or occurrence table in datapackage.json; writing empty verbatim",
          datasetId);
      records = spark.emptyDataset(Encoders.bean(ExtendedRecord.class));
    }

    if (ingestPlan != null) {
      writeIngestPlans(fileSystem, parquetBasePath, mappingEngine, ingestPlan, dataPackage);
    }

    records.persist(StorageLevel.MEMORY_AND_DISK());
    VerbatimConversionMetrics metrics;
    try {
      String tempOutputPath = verbatimOutputPath + ".parts";
      records
          .coalesce(1)
          .write()
          .mode(SaveMode.Overwrite)
          .format("avro")
          .option("avroSchema", EXTENDED_RECORD_SCHEMA.toString())
          .save(tempOutputPath);

      mergeToSingleFile(fileSystem, tempOutputPath, verbatimOutputPath);

      metrics =
          writeMetrics(
              spark,
              dataPackage,
              parquetBasePath,
              fileSystem,
              datasetId,
              Optional.of(records),
              branchMetrics);
    } finally {
      records.unpersist(false);
      if (mappingExecution != null) {
        mappingExecution.close();
      }
    }

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
    DwcDpMappingEngine mappingEngine = DwcDpMappingEngine.currentSchema();
    return mappingEngine.execute(
        parquetTableLoader(spark, dataPackage, basePath),
        EventDwcaMapping.current(mappingEngine.schemaGraph()),
        dataPackage);
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
    DwcDpMappingEngine mappingEngine = DwcDpMappingEngine.currentSchema();
    return mappingEngine.execute(
        parquetTableLoader(spark, dataPackage, basePath),
        OccurrenceDwcaMapping.current(mappingEngine.schemaGraph()),
        dataPackage);
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

  static void writeIngestPlans(
      FileSystem fileSystem,
      String datasetBasePath,
      DwcDpMappingEngine mappingEngine,
      MappingPlan plan,
      DataPackage dataPackage) {
    writeTextFile(
        fileSystem,
        datasetBasePath + "/" + INGEST_PLAN_COMPACT,
        mappingEngine.targetPlan(plan, dataPackage));
    writeTextFile(
        fileSystem,
        datasetBasePath + "/" + INGEST_PLAN_DETAILED,
        mappingEngine.targetPlanDetailed(plan, dataPackage));
  }

  private static void writeTextFile(FileSystem fileSystem, String path, String content) {
    org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(path);
    try (BufferedWriter writer =
        new BufferedWriter(
            new OutputStreamWriter(
                fileSystem.create(outputPath, true), StandardCharsets.UTF_8))) {
      writer.write(content);
      if (!content.endsWith("\n")) {
        writer.newLine();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to write DwC-DP ingest plan " + path, e);
    }
  }

  static VerbatimConversionMetrics writeMetrics(
      SparkSession spark,
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId) {
    return writeMetrics(
        spark,
        dataPackage,
        datasetBasePath,
        fileSystem,
        datasetId,
        Optional.empty(),
        List.of());
  }

  static VerbatimConversionMetrics writeMetrics(
      SparkSession spark,
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId,
      Optional<Dataset<ExtendedRecord>> verbatimDataset) {
    return writeMetrics(
        spark,
        dataPackage,
        datasetBasePath,
        fileSystem,
        datasetId,
        verbatimDataset,
        List.of());
  }

  static VerbatimConversionMetrics writeMetrics(
      SparkSession spark,
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId,
      Optional<Dataset<ExtendedRecord>> verbatimDataset,
      List<MappingBranchExecutionMetrics> branchMetrics) {

    Map<String, Long> sourceCounts = sourceCounts(spark, dataPackage, datasetBasePath);
    long occurrenceCount = sourceCounts.getOrDefault("occurrence", 0L);
    long eventCount = sourceCounts.getOrDefault("event", 0L);
    long largestFileCount = sourceCounts.values().stream().mapToLong(Long::longValue).max().orElse(0L);

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
        dataPackage,
        datasetBasePath,
        fileSystem,
        datasetId,
        sourceCounts,
        verbatimDataset,
        branchMetrics);

    return new VerbatimConversionMetrics(0L, occurrenceCount, eventCount, largestFileCount);
  }

  private static Map<String, Long> sourceCounts(
      SparkSession spark, DataPackage dataPackage, String datasetBasePath) {
    Map<String, Long> counts = new LinkedHashMap<>();
    dataPackage.getResources().stream()
        .sorted(Comparator.comparing(DataPackageResource::getName))
        .forEach(
            resource -> counts.put(resource.getName(), countRows(spark, datasetBasePath, resource)));
    return counts;
  }

  private static void writeConversionReport(
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId,
      Map<String, Long> sourceCounts,
      Optional<Dataset<ExtendedRecord>> verbatimDataset,
      List<MappingBranchExecutionMetrics> branchMetrics) {
    List<String> lines = new ArrayList<>();
    lines.add("DwC-DP conversion report: " + datasetId);
    lines.add("");
    lines.add("source tables (raw row counts):");
    sourceCounts.forEach((resource, count) -> lines.add("  " + resource + ": " + count));

    lines.add("");
    lines.add("mapping branches (execution funnels):");
    if (branchMetrics.isEmpty()) {
      lines.add("  (execution metrics not supplied)");
    } else {
      appendBranchMetrics(lines, branchMetrics);
    }

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

  private static void appendBranchMetrics(
      List<String> lines, List<MappingBranchExecutionMetrics> branchMetrics) {
    for (MappingBranchExecutionMetrics branch : branchMetrics) {
      lines.add("  " + branch.branchName());
      int relationNumber = 1;
      for (RelationExecutionMetrics relation : branch.relations()) {
        long singleMatch =
            Math.max(0L, relation.matchedParentRows() - relation.multipleMatchParentRows());
        lines.add(
            "    "
                + relationNumber++
                + ". "
                + relation.sourceResource()
                + " -> "
                + relation.targetResource()
                + " ["
                + relation.cardinality()
                + ", "
                + relation.requirement()
                + (relation.filtered() ? ", FILTERED" : "")
                + (relation.skipped() ? ", SKIPPED" : "")
                + "]");
        lines.add(
            "       parents: input="
                + relation.inputRows()
                + ", key-present="
                + relation.sourceKeyPresentRows()
                + ", matched="
                + relation.matchedParentRows()
                + ", single-match="
                + singleMatch
                + ", multi-match="
                + relation.multipleMatchParentRows()
                + ", unmatched="
                + relation.unmatchedParentRows());
        lines.add(
            "       target: before-filter="
                + relation.targetRowsBeforeFilter()
                + ", after-filter="
                + relation.targetRowsAfterFilter()
                + ", output-rows="
                + relation.outputRows());
      }
    }
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
