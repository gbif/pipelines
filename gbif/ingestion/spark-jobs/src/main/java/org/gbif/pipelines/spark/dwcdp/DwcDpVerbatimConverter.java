package org.gbif.pipelines.spark.dwcdp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.builder.extension.AgentJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.AssertionExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.GeologicalContextJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.HumboldtExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentificationJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.JoinFunnel;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialGeologicalContextJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialProtocolJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialProvenanceJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.OrganismJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.ProtocolJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.ProvenanceJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.UsagePolicyJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageResource;
import org.gbif.pipelines.spark.util.PathUtil;
import org.gbif.pipelines.spark.util.TableLoader;
import scala.Tuple2;

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

  /**
   * Everything {@link #writeConversionReport} needs to render {@code conversion-report.txt},
   * bundled into one holder instead of individual positional parameters. This is what keeps the
   * report appendable: adding a new section (e.g. a new builder's {@link JoinFunnel}) means adding
   * an entry to {@code joinFunnels} at the {@link #writeMetrics} call site — {@link
   * #writeConversionReport}'s signature never needs to change again to accommodate it.
   *
   * @param joinFunnels one entry per join/enrichment builder that was able to compute a funnel
   *     (empty unless {@code detailedReportEnabled}); order is preserved in the rendered report
   * @param detailedReportEnabled mirrors {@code PipelinesConfig#isDwcdpDetailedConversionReport()}
   *     — governs only whether {@link #writeMetrics} attempted to compute {@code joinFunnels}, not
   *     whether the report itself is written; recorded here so the report can tell a reader
   *     "nothing to show" apart from "detail was never turned on"
   */
  private record ConversionReportData(
      String datasetId,
      Map<String, Long> tableRowCounts,
      long eventCount,
      long physicalOccurrenceCount,
      long virtualOccurrenceCount,
      Optional<MaterialJoinBuilder.MaterialFunnel> materialFunnel,
      Optional<Long> coreRecordCount,
      Optional<Map<String, long[]>> extensionSummary,
      List<JoinFunnel> joinFunnels,
      boolean detailedReportEnabled) {}

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

    // Re-read the already-written output rather than reusing the lazy `records` Dataset: `records`
    // is an unevaluated pipeline (EventCoreBuilder/OccurrenceCoreBuilder's joins), so passing it
    // to writeMetrics for the extension summary below would re-run the entire build from scratch a
    // second time. Reading back the single merged Avro file is a cheap re-scan of
    // already-materialised
    // output instead.
    Dataset<ExtendedRecord> writtenRecords =
        spark
            .read()
            .format("avro")
            .load(verbatimOutputPath)
            .as(Encoders.bean(ExtendedRecord.class));

    VerbatimConversionMetrics metrics =
        writeMetrics(
            spark,
            dataPackage,
            parquetBasePath,
            fileSystem,
            datasetId,
            Optional.of(writtenRecords),
            config.isDwcdpDetailedConversionReport());

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
    return writeMetrics(
        spark, dataPackage, datasetBasePath, fileSystem, datasetId, Optional.empty(), false);
  }

  /**
   * Overload used by {@link #convert} once the {@link ExtendedRecord} dataset has been built, so
   * the conversion report can include a section on what actually ended up in the written output —
   * not just what the source tables contained. The 5-arg {@link #writeMetrics} overload (used
   * directly by tests that don't build a full {@code records} dataset) delegates here with {@code
   * records} absent, in which case the report simply omits that section.
   *
   * <p>Detailed join-builder funnels are off ({@code detailedReport=false}) via this overload —
   * existing callers (tests included) that don't care about join funnels keep working unchanged.
   * Use the 7-arg overload to opt in.
   */
  static VerbatimConversionMetrics writeMetrics(
      SparkSession spark,
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId,
      Optional<Dataset<ExtendedRecord>> records) {
    return writeMetrics(spark, dataPackage, datasetBasePath, fileSystem, datasetId, records, false);
  }

  /**
   * Full overload — adds {@code detailedReport}, which gates computation of the per-builder {@link
   * JoinFunnel} breakdowns (see {@link PipelinesConfig#isDwcdpDetailedConversionReport()}). Each
   * funnel is an extra Spark pass purely for reporting, so it's opt-in rather than always computed
   * alongside the material funnel and extension summary, which are cheap enough to always include.
   */
  static VerbatimConversionMetrics writeMetrics(
      SparkSession spark,
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId,
      Optional<Dataset<ExtendedRecord>> records,
      boolean detailedReport) {

    long physicalOccurrenceCount =
        dataPackage
            .findResource("occurrence")
            .map(r -> countRows(spark, datasetBasePath, r))
            .orElse(0L);

    // Event-core packages can materialise additional occurrence extension rows from material
    // records. This count is consumed by the coordinator and balancer to decide whether the
    // occurrence workflow must run, so it must describe the transformed archive rather than only
    // the physical occurrence resource.
    TableLoader loader =
        tableName ->
            dataPackage
                .findResource(tableName)
                .map(r -> spark.read().parquet(datasetBasePath + "/" + r.getPath()));
    long virtualOccurrenceCount =
        MaterialJoinBuilder.virtualMaterialOccurrences(loader).map(Dataset::count).orElse(0L);
    long occurrenceCount = physicalOccurrenceCount + virtualOccurrenceCount;
    if (log.isDebugEnabled()) {
      log.debug(
          "Occurrence counts for dataset {}: physical={}, virtual={}, total={}",
          datasetId,
          physicalOccurrenceCount,
          virtualOccurrenceCount,
          occurrenceCount);
    }

    long eventCount =
        dataPackage.findResource("event").map(r -> countRows(spark, datasetBasePath, r)).orElse(0L);

    // Raw row count for every table declared in datapackage.json, regardless of whether this
    // conversion's builder path actually reads it — this is the "what did we start with" half of
    // the report, independent of any specific extension's join/filter logic.
    Map<String, Long> tableRowCounts = new LinkedHashMap<>();
    for (DataPackageResource resource : dataPackage.getResources()) {
      tableRowCounts.put(resource.getName(), countRows(spark, datasetBasePath, resource));
    }
    long largestFileCount =
        tableRowCounts.values().stream().mapToLong(Long::longValue).max().orElse(0L);

    Optional<MaterialJoinBuilder.MaterialFunnel> materialFunnel =
        MaterialJoinBuilder.computeFunnel(loader);
    Optional<Map<String, long[]>> extensionSummary =
        records.map(DwcDpVerbatimConverter::summarizeExtensions);
    Optional<Long> coreRecordCount = records.map(Dataset::count);

    // Each join builder's computeFunnel is an extra Spark pass purely for reporting, so it's only
    // run when detailedReport is turned on for this dataset/attempt — see
    // PipelinesConfig#isDwcdpDetailedConversionReport(). No builders wired in yet; each gets
    // appended here as its computeFunnel method is added, with no change needed to
    // writeConversionReport's signature.
    List<JoinFunnel> joinFunnels = detailedReport ? computeJoinFunnels(loader) : List.of();

    writeConversionReport(
        fileSystem,
        datasetBasePath,
        new ConversionReportData(
            datasetId,
            tableRowCounts,
            eventCount,
            physicalOccurrenceCount,
            virtualOccurrenceCount,
            materialFunnel,
            coreRecordCount,
            extensionSummary,
            joinFunnels,
            detailedReport));

    Map<String, Long> metrics =
        Map.of(
            Metrics.ARCHIVE_TO_ER_COUNT, 0L,
            Metrics.ARCHIVE_TO_OCC_COUNT, occurrenceCount,
            Metrics.EVENT_CORE_RECORDS_COUNT, eventCount,
            Metrics.ARCHIVE_TO_LARGEST_FILE_COUNT, largestFileCount);

    String metricsPath = datasetBasePath + "/" + Pipeline.ARCHIVE_TO_VERBATIM + ".yml";
    log.debug("Writing verbatim metrics for dataset {}: {}", datasetId, metrics);
    MetricsUtil.writeMetricsYaml(fileSystem, metrics, metricsPath);

    return new VerbatimConversionMetrics(0L, occurrenceCount, eventCount, largestFileCount);
  }

  /**
   * Distributed summary of what actually ended up in the written {@link ExtendedRecord}s: for each
   * extension row type present anywhere in the dataset, the total number of extension rows across
   * all core records and how many core records carry at least one row of that type. Computed
   * directly from the built dataset rather than from source tables or join logic, so it reflects
   * the true output regardless of which builder produced it — this is the "what did we end up with"
   * half of the report, to compare against {@code tableRowCounts}.
   *
   * <p>Deliberately uses the plain RDD API ({@code flatMapToPair}/{@code reduceByKey}) rather than
   * building a {@code Dataset<Row>}: constructing a bare {@code Row} encoder by hand ({@code
   * RowEncoder.apply}/{@code encoderFor}) has churned across Spark 3.x versions and isn't reliably
   * Java-callable. The RDD API needs no {@link org.apache.spark.sql.Encoder} at all, so it's immune
   * to that churn.
   */
  private static Map<String, long[]> summarizeExtensions(Dataset<ExtendedRecord> records) {
    JavaPairRDD<String, long[]> perRecordCounts =
        records
            .toJavaRDD()
            .flatMapToPair(
                (PairFlatMapFunction<ExtendedRecord, String, long[]>)
                    r -> {
                      List<Tuple2<String, long[]>> out = new ArrayList<>();
                      Map<String, List<Map<String, String>>> ext = r.getExtensions();
                      if (ext != null) {
                        for (Map.Entry<String, List<Map<String, String>>> e : ext.entrySet()) {
                          int size = e.getValue() == null ? 0 : e.getValue().size();
                          if (size > 0) {
                            // [rows contributed by this record, 1 record carrying this extension]
                            out.add(new Tuple2<>(e.getKey(), new long[] {size, 1}));
                          }
                        }
                      }
                      return out.iterator();
                    });

    Map<String, long[]> summary = new LinkedHashMap<>();
    for (Tuple2<String, long[]> entry :
        perRecordCounts.reduceByKey((a, b) -> new long[] {a[0] + b[0], a[1] + b[1]}).collect()) {
      summary.put(entry._1(), entry._2());
    }
    return summary;
  }

  /**
   * Renders and persists a human-readable conversion report covering the whole conversion, not just
   * the material/virtual-occurrence path:
   *
   * <ul>
   *   <li>raw row counts for every table declared in {@code datapackage.json}
   *   <li>core output counts (events, physical/virtual occurrences)
   *   <li>the {@link MaterialJoinBuilder.MaterialFunnel} breakdown, when a {@code material} table
   *       is present
   *   <li>a summary of every extension row type that actually made it into the written {@link
   *       ExtendedRecord}s, when that dataset was available to {@link #writeMetrics}
   *   <li>one block per entry in {@code data.joinFunnels()}, when detailed reporting was enabled
   * </ul>
   *
   * <p>Purely diagnostic: written to {@code conversion-report.txt} alongside {@code
   * archive-to-verbatim.yml}, and logged at INFO whenever a material funnel, extension summary, or
   * join funnel is present (the cases where rows can silently go missing), DEBUG otherwise. Never
   * read by the coordinator or balancer, so it's safe to extend without touching routing behaviour.
   *
   * <p>Takes a single {@link ConversionReportData} rather than individual parameters specifically
   * so that adding a new report section — most likely a new entry in {@code joinFunnels} — never
   * requires touching this method's signature again.
   */
  private static void writeConversionReport(
      FileSystem fileSystem, String datasetBasePath, ConversionReportData data) {

    StringBuilder sb = new StringBuilder();
    sb.append("Conversion report for dataset ").append(data.datasetId()).append(":\n");

    sb.append("  source tables (raw row counts):\n");
    data.tableRowCounts()
        .forEach((name, count) -> sb.append(String.format("    %-28s%d%n", name + ":", count)));

    sb.append("  core output:\n");
    sb.append(String.format("    %-28s%d%n", "events:", data.eventCount()));
    sb.append(
        String.format("    %-28s%d%n", "occurrences (physical):", data.physicalOccurrenceCount()));
    sb.append(
        String.format("    %-28s%d%n", "occurrences (virtual):", data.virtualOccurrenceCount()));
    sb.append(
        String.format(
            "    %-28s%d%n",
            "occurrences (total):",
            data.physicalOccurrenceCount() + data.virtualOccurrenceCount()));
    data.coreRecordCount()
        .ifPresent(c -> sb.append(String.format("    %-28s%d%n", "core records written:", c)));

    data.materialFunnel()
        .ifPresent(
            f -> {
              sb.append("  material funnel:\n");
              sb.append(String.format("    %-28s%d%n", "material rows (total):", f.total()));
              sb.append(String.format("    %-28s%d%n", "  with evidence:", f.withEvidence()));
              sb.append(
                  String.format(
                      "    %-28s%d%n",
                      "    -> enriched real occurrence:", f.enrichedOntoRealOccurrence()));
              sb.append(
                  String.format(
                      "    %-28s%d%n", "    -> ambiguous, DROPPED:", f.evidenceAmbiguous()));
              sb.append(String.format("    %-28s%d%n", "  without evidence:", f.withoutEvidence()));
              sb.append(
                  String.format("    %-28s%d%n", "    -> became virtual occurrence:", f.virtual()));
              sb.append(
                  String.format("    %-28s%d%n", "    -> unresolved, DROPPED:", f.unresolved()));
            });

    data.extensionSummary()
        .ifPresent(
            summary -> {
              sb.append("  output extensions (rows actually written):\n");
              if (summary.isEmpty()) {
                sb.append("    (none)\n");
              }
              summary.forEach(
                  (rowType, counts) ->
                      sb.append(
                          String.format(
                              "    %-42s rows=%-8d records-with-this-ext=%d%n",
                              rowType + ":", counts[0], counts[1])));
            });

    if (data.detailedReportEnabled()) {
      sb.append("  join funnels (detailed reporting enabled):\n");
      if (data.joinFunnels().isEmpty()) {
        sb.append("    (none of the join builders had a computable funnel for this dataset)\n");
      }
      for (JoinFunnel funnel : data.joinFunnels()) {
        sb.append("    ").append(funnel.label()).append(":\n");
        for (JoinFunnel.Bucket bucket : funnel.buckets()) {
          sb.append(String.format("      %-26s%d%n", bucket.name() + ":", bucket.count()));
        }
      }
    } else {
      sb.append(
          "  join funnels: not computed (set dwcdpDetailedConversionReport: true to enable "
              + "for this dataset/attempt)\n");
    }

    String report = sb.toString();
    boolean noteworthy =
        data.materialFunnel().isPresent()
            || data.extensionSummary().isPresent()
            || !data.joinFunnels().isEmpty();
    if (noteworthy) {
      log.info("\n{}", report);
    } else {
      log.debug("\n{}", report);
    }

    String reportPath = datasetBasePath + "/conversion-report.txt";
    try {
      FsUtils.createFile(fileSystem, reportPath, report);
    } catch (IOException e) {
      log.warn("Failed to write conversion report to {}", reportPath, e);
    }
  }

  /**
   * Computes the {@link JoinFunnel} breakdown for every join/enrichment builder that supports one,
   * skipping any builder whose relevant source table is absent from this package (each {@code
   * computeFunnel} already returns {@link Optional#empty()} in that case). Only called when {@code
   * detailedReport} is enabled — see {@link #writeMetrics}.
   *
   * <p>This is the single place a new builder's funnel gets appended as it's implemented, with no
   * change required anywhere else in this class.
   */
  private static List<JoinFunnel> computeJoinFunnels(TableLoader loader) {
    List<JoinFunnel> funnels = new ArrayList<>();

    AgentJoinBuilder.computeFunnel(loader, "event", "eventConductedByID", "eventConductedBy")
        .ifPresent(funnels::add);
    AgentJoinBuilder.computeFunnel(loader, "event", "georeferencedByID", "georeferencedBy")
        .ifPresent(funnels::add);
    AgentJoinBuilder.computeFunnel(loader, "occurrence", "recordedByID", "recordedBy")
        .ifPresent(funnels::add);
    AgentJoinBuilder.computeFunnel(loader, "occurrence", "identifiedByID", "identifiedBy")
        .ifPresent(funnels::add);

    ProtocolJoinBuilder.computeFunnel(loader, "event", "eventProtocol_fk", "samplingProtocol")
        .ifPresent(funnels::add);
    ProtocolJoinBuilder.computeFunnel(
            loader, "occurrence", "occurrenceProtocol_fk", "samplingProtocol")
        .ifPresent(funnels::add);

    ProvenanceJoinBuilder.computeFunnel(loader).ifPresent(funnels::add);

    GeologicalContextJoinBuilder.computeFunnel(loader).ifPresent(funnels::add);
    OrganismJoinBuilder.computeFunnel(loader).ifPresent(funnels::add);
    UsagePolicyJoinBuilder.computeFunnel(loader, "media").ifPresent(funnels::add);
    UsagePolicyJoinBuilder.computeFunnel(loader, "material").ifPresent(funnels::add);
    IdentificationJoinBuilder.computeFunnel(loader).ifPresent(funnels::add);

    MaterialProtocolJoinBuilder.computeFunnel(loader).ifPresent(funnels::add);
    MaterialProvenanceJoinBuilder.computeFunnel(loader).ifPresent(funnels::add);
    MaterialGeologicalContextJoinBuilder.computeFunnel(loader).ifPresent(funnels::add);

    return funnels;
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
