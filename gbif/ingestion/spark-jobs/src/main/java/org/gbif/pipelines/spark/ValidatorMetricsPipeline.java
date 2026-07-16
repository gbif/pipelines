package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;
import static org.gbif.pipelines.spark.Directories.OCCURRENCE_JSON;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.spark.pojo.SparkValidatorMetrics;
import org.gbif.pipelines.spark.util.SingleDatasetPipelineArgs;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Metrics.IssueSample;

/**
 * Spark pipeline that computes validator metrics directly from interpreted occurrence Parquet
 * files. Writes {@value #METRICS_FILENAME} to HDFS, which {@code SparkMetricsCollector} reads
 * instead of querying Elasticsearch.
 */
@Slf4j
public class ValidatorMetricsPipeline {

  public static final String METRICS_FILENAME = "collect-metrics.json";

  private static final int MAX_SAMPLES = 5;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Marker prefix for taxonomy column references that need special Spark access. */
  private static final String TAXONOMY_PREFIX = "TAXONOMY:";

  private static final String GBIF_CHECKLIST_KEY = "GBIF";

  @Parameters(separators = "=")
  private static class Args extends SingleDatasetPipelineArgs {}

  public static void main(String[] argsv) throws Exception {
    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    org.gbif.pipelines.core.config.model.PipelinesConfig config = loadConfig(args.config);
    SparkSession spark = null;
    FileSystem fileSystem = null;

    try {
      spark = getSparkSession(args.master, args.appName, config, (b, c) -> {});
      fileSystem = getFileSystem(spark, config);

      String outputPath =
          String.format("%s/%s/%d", config.getOutputPath(), args.datasetId, args.attempt);
      run(spark, fileSystem, outputPath);
    } finally {
      if (fileSystem != null) fileSystem.close();
      if (spark != null) {
        spark.stop();
        spark.close();
      }
    }

    if (args.useSystemExit) {
      System.exit(0);
    }
  }

  /**
   * Maps DWC term qualified names to a column specifier. Non-taxonomy entries use the direct
   * OccurrenceJsonRecord field name; taxonomy entries use {@code TAXONOMY:<rank>} to indicate the
   * value lives in {@code classifications["GBIF"].classification[rank]}.
   */
  private static final Map<String, String> TERM_TO_COLUMN = buildTermToColumn();

  private static Map<String, String> buildTermToColumn() {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("http://rs.tdwg.org/dwc/terms/basisOfRecord", "basisOfRecord");
    m.put("http://rs.tdwg.org/dwc/terms/sex", "sex");
    m.put("http://rs.tdwg.org/dwc/terms/lifeStage", "lifeStage");
    m.put("http://rs.tdwg.org/dwc/terms/establishmentMeans", "establishmentMeans");
    m.put("http://rs.tdwg.org/dwc/terms/individualCount", "individualCount");
    m.put("http://rs.tdwg.org/dwc/terms/typeStatus", "typeStatus");
    m.put("http://purl.org/dc/terms/references", "references");
    m.put("http://rs.tdwg.org/dwc/terms/preparations", "preparations");
    m.put("http://rs.tdwg.org/dwc/terms/minimumElevationInMeters", "minimumElevationInMeters");
    m.put("http://rs.tdwg.org/dwc/terms/maximumElevationInMeters", "maximumElevationInMeters");
    m.put("http://rs.tdwg.org/dwc/terms/minimumDepthInMeters", "minimumDepthInMeters");
    m.put("http://rs.tdwg.org/dwc/terms/maximumDepthInMeters", "maximumDepthInMeters");
    m.put(
        "http://rs.tdwg.org/dwc/terms/minimumDistanceAboveSurfaceInMeters",
        "minimumDistanceAboveSurfaceInMeters");
    m.put(
        "http://rs.tdwg.org/dwc/terms/maximumDistanceAboveSurfaceInMeters",
        "maximumDistanceAboveSurfaceInMeters");
    m.put(
        "http://rs.tdwg.org/dwc/terms/coordinateUncertaintyInMeters",
        "coordinateUncertaintyInMeters");
    m.put("http://rs.tdwg.org/dwc/terms/coordinatePrecision", "coordinatePrecision");
    m.put("http://rs.tdwg.org/dwc/terms/decimalLatitude", "decimalLatitude");
    m.put("http://rs.tdwg.org/dwc/terms/decimalLongitude", "decimalLongitude");
    m.put("http://rs.tdwg.org/dwc/terms/year", "year");
    m.put("http://rs.tdwg.org/dwc/terms/month", "month");
    m.put("http://rs.tdwg.org/dwc/terms/day", "day");
    m.put("http://rs.tdwg.org/dwc/terms/startDayOfYear", "startDayOfYear");
    m.put("http://rs.tdwg.org/dwc/terms/endDayOfYear", "endDayOfYear");
    m.put("http://purl.org/dc/terms/modified", "modified");
    m.put("http://rs.tdwg.org/dwc/terms/dateIdentified", "dateIdentified");
    m.put("http://rs.tdwg.org/dwc/terms/eventDate", "eventDateSingle");
    m.put("http://rs.tdwg.org/dwc/terms/occurrenceStatus", "occurrenceStatus");
    m.put("http://rs.tdwg.org/dwc/terms/organismQuantity", "organismQuantity");
    m.put("http://rs.tdwg.org/dwc/terms/organismQuantityType", "organismQuantityType");
    m.put("http://rs.tdwg.org/dwc/terms/sampleSizeUnit", "sampleSizeUnit");
    m.put("http://rs.tdwg.org/dwc/terms/sampleSizeValue", "sampleSizeValue");
    m.put("http://rs.tdwg.org/dwc/terms/continent", "continent");
    m.put("http://rs.tdwg.org/dwc/terms/recordedBy", "recordedBy");
    m.put("http://rs.tdwg.org/dwc/terms/identifiedBy", "identifiedBy");
    m.put("http://rs.tdwg.org/dwc/terms/recordNumber", "recordNumber");
    m.put("http://rs.tdwg.org/dwc/terms/organismID", "organismId");
    m.put("http://rs.tdwg.org/dwc/terms/samplingProtocol", "samplingProtocol");
    m.put("http://rs.tdwg.org/dwc/terms/eventID", "eventId");
    m.put("http://rs.tdwg.org/dwc/terms/parentEventID", "parentEventId");
    m.put("http://rs.tdwg.org/dwc/terms/institutionCode", "institutionCode");
    m.put("http://rs.tdwg.org/dwc/terms/locality", "locality");
    m.put("http://rs.tdwg.org/dwc/terms/occurrenceID", "occurrenceId");
    m.put("http://rs.tdwg.org/dwc/terms/waterBody", "waterBody");
    m.put("http://rs.tdwg.org/dwc/terms/countryCode", "countryCode");
    m.put("http://rs.tdwg.org/dwc/terms/country", "country");
    m.put("http://rs.tdwg.org/dwc/terms/stateProvince", "stateProvince");
    // Taxonomy fields - stored under classifications["GBIF"].classification[rank]
    m.put("http://rs.tdwg.org/dwc/terms/kingdom", TAXONOMY_PREFIX + "kingdom");
    m.put("http://rs.tdwg.org/dwc/terms/phylum", TAXONOMY_PREFIX + "phylum");
    m.put("http://rs.tdwg.org/dwc/terms/class", TAXONOMY_PREFIX + "class");
    m.put("http://rs.tdwg.org/dwc/terms/order", TAXONOMY_PREFIX + "order");
    m.put("http://rs.tdwg.org/dwc/terms/family", TAXONOMY_PREFIX + "family");
    m.put("http://rs.tdwg.org/dwc/terms/genus", TAXONOMY_PREFIX + "genus");
    m.put("http://rs.tdwg.org/dwc/terms/taxonID", TAXONOMY_PREFIX + "taxonID");
    return Collections.unmodifiableMap(m);
  }

  /**
   * Runs the pipeline: reads interpreted occurrence JSON Parquet from {@code outputPath/json},
   * computes metrics, and writes them as JSON to {@code outputPath/collect-metrics.json}.
   */
  public static void run(SparkSession spark, FileSystem fs, String outputPath) throws IOException {
    log.info("Running ValidatorMetricsPipeline for {}", outputPath);

    Dataset<OccurrenceJsonRecord> records =
        spark
            .read()
            .parquet(outputPath + "/" + OCCURRENCE_JSON)
            .as(Encoders.bean(OccurrenceJsonRecord.class));

    records.cache();
    try {
      long indexedCount = records.count();
      log.info("Indexed count: {}", indexedCount);

      List<IssueInfo> issues = computeIssues(records);
      log.info("Computed {} distinct issues", issues.size());

      Map<String, Long> interpretedFieldCounts = computeInterpretedFieldCounts(records);
      log.info("Computed interpreted counts for {} terms", interpretedFieldCounts.size());

      SparkValidatorMetrics result =
          SparkValidatorMetrics.builder()
              .indexedCount(indexedCount)
              .issues(issues)
              .interpretedFieldCounts(interpretedFieldCounts)
              .build();

      String json = MAPPER.writeValueAsString(result);
      FsUtils.createFile(fs, outputPath + "/" + METRICS_FILENAME, json);
      log.info("Written validator metrics to {}/{}", outputPath, METRICS_FILENAME);
    } finally {
      records.unpersist();
    }
  }

  /**
   * Aggregates issues from the records, computing a total count and up to {@link #MAX_SAMPLES}
   * sample records per issue. Samples include only verbatim values for terms related to the issue.
   */
  private static List<IssueInfo> computeIssues(Dataset<OccurrenceJsonRecord> records) {
    // Explode the issues list so each (record, issue) pair becomes a row
    Dataset<Row> exploded =
        records.select(
            col("id"),
            col("verbatim.core").alias("verbatimCore"),
            explode(col("issues")).alias("issue"));

    // Count occurrences per issue across the full dataset
    Dataset<Row> issueCounts = exploded.groupBy("issue").agg(count("*").alias("issueCount"));

    // Use a window to rank records within each issue partition and keep only the top MAX_SAMPLES
    WindowSpec window = Window.partitionBy("issue").orderBy("id");
    Dataset<Row> ranked = exploded.withColumn("rn", row_number().over(window));
    Dataset<Row> limited = ranked.filter(col("rn").leq(MAX_SAMPLES));

    Dataset<Row> groupedSamples =
        limited
            .groupBy("issue")
            .agg(collect_list(struct(col("id"), col("verbatimCore"))).alias("samples"));

    Dataset<Row> joined = issueCounts.join(groupedSamples, "issue");

    return joined.collectAsList().stream()
        .map(ValidatorMetricsPipeline::rowToIssueInfo)
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static IssueInfo rowToIssueInfo(Row row) {
    String issueName = row.getString(row.fieldIndex("issue"));
    long count = row.getLong(row.fieldIndex("issueCount"));
    List<Row> rawSamples = row.getList(row.fieldIndex("samples"));

    Set<Term> relatedTerms = Collections.emptySet();
    try {
      relatedTerms = OccurrenceIssue.valueOf(issueName).getRelatedTerms();
    } catch (IllegalArgumentException ex) {
      log.warn("Unknown OccurrenceIssue: {}", issueName);
    }

    final Set<Term> terms = relatedTerms;
    List<IssueSample> samples =
        rawSamples.stream()
            .map(
                sampleRow -> {
                  String recordId = sampleRow.getString(0);
                  Map<String, String> verbatimCore = sampleRow.getJavaMap(1);
                  Map<String, String> relatedData = new HashMap<>();
                  for (Term term : terms) {
                    String value =
                        verbatimCore != null ? verbatimCore.get(term.qualifiedName()) : null;
                    if (value != null && !value.trim().isEmpty()) {
                      relatedData.put(term.toString(), value);
                    }
                  }
                  return IssueSample.builder().recordId(recordId).relatedData(relatedData).build();
                })
            .collect(Collectors.toList());

    return IssueInfo.builder()
        .issue(issueName)
        .count(count)
        .issueCategory(EvaluationCategory.OCC_INTERPRETATION_BASED)
        .samples(samples)
        .build();
  }

  /**
   * Counts non-null interpreted values for each term in {@link #TERM_TO_COLUMN} in a single Spark
   * aggregation pass. Returns a map keyed by ES field path (matching {@code
   * RawToInterpreted.getInterpretedField} values) so that {@code SparkMetricsCollector} can
   * cross-reference without duplicating the mapping.
   */
  private static Map<String, Long> computeInterpretedFieldCounts(
      Dataset<OccurrenceJsonRecord> records) {

    List<String> termQualifiedNames = new ArrayList<>(TERM_TO_COLUMN.keySet());
    if (termQualifiedNames.isEmpty()) {
      return Collections.emptyMap();
    }

    // Build one count(when(col.isNotNull, 1)) per term, aliased by ordinal index
    Column firstAgg = null;
    List<Column> restAggs = new ArrayList<>();
    for (int i = 0; i < termQualifiedNames.size(); i++) {
      String term = termQualifiedNames.get(i);
      Column colExpr = resolveColumn(TERM_TO_COLUMN.get(term));
      Column agg = count(when(colExpr.isNotNull(), 1)).alias("t" + i);
      if (i == 0) {
        firstAgg = agg;
      } else {
        restAggs.add(agg);
      }
    }

    Row countRow = records.agg(firstAgg, restAggs.toArray(new Column[0])).first();

    Map<String, Long> result = new HashMap<>();
    for (int i = 0; i < termQualifiedNames.size(); i++) {
      result.put(termQualifiedNames.get(i), countRow.getLong(i));
    }
    return result;
  }

  /**
   * Resolves a column specifier from {@link #TERM_TO_COLUMN} to a Spark {@link Column}.
   *
   * <p>Taxonomy specifiers use the form {@code TAXONOMY:<rank>} and are resolved to {@code
   * classifications["GBIF"].classification[rank]}.
   */
  private static Column resolveColumn(String columnSpec) {
    if (columnSpec.startsWith(TAXONOMY_PREFIX)) {
      String rank = columnSpec.substring(TAXONOMY_PREFIX.length());
      return element_at(
          element_at(col("classifications"), GBIF_CHECKLIST_KEY).getField("classification"), rank);
    }
    return col(columnSpec);
  }
}
