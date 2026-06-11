package org.gbif.pipelines.spark.dwcdp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.PathUtil;

/**
 * Converts DwC-DP Parquet files (written by DataPackageConversionPipeline) into verbatim.avro.
 *
 * <p>Strategy:
 *
 * <ol>
 *   <li>Read datapackage.json to discover present tables and their HDFS paths.
 *   <li>If event table present: event-core records, occurrences as extension, media as extension.
 *   <li>Else if occurrence table present: occurrence-core records, media as extension.
 *   <li>Build joins distributed on Spark executors.
 *   <li>Write using Spark's built-in Avro writer — fully distributed, no driver-side collection.
 * </ol>
 *
 * <p>Output is written to {@code {inputPath}/{datasetId}/{attempt}/verbatim.avro}, the same path
 * that IdentifiersPipeline and EventInterpretationPipeline read from via {@code
 * spark.read().format("avro").load(path + "/verbatim.avro")}.
 */
@Slf4j
public class DwcDpVerbatimConverter {

  // Well-known DwC-DP table names as they appear in datapackage.json
  public static final String TABLE_EVENT = "event";
  public static final String TABLE_OCCURRENCE = "occurrence";
  public static final String TABLE_MEDIA = "media";
  public static final String TABLE_EVENT_MEDIA = "event-media";
  public static final String TABLE_OCCURRENCE_MEDIA = "occurrence-media";

  // Extension row type URI for multimedia
  public static final String ROW_TYPE_MULTIMEDIA = "http://rs.tdwg.org/ac/terms/Multimedia";

  // Core row type URIs
  public static final String CORE_ROW_TYPE_EVENT = DwcTerm.Event.qualifiedName();
  public static final String CORE_ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  // Extension row type for occurrences attached to an event core
  public static final String ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  private static final TermFactory TERM_FACTORY = TermFactory.instance();
  private static final ObjectMapper MAPPER = MapperUtil.MAPPER;

  private DwcDpVerbatimConverter() {}

  // ---- public entry point ----

  public static void convert(
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

    Dataset<ExtendedRecord> records;

    if (containsEvents && dataPackage.findResource(TABLE_EVENT).isPresent()) {
      log.info("Building event-core ExtendedRecords");
      records = buildEventCoreDataset(spark, dataPackage, parquetBasePath);
    } else if (containsOccurrences && dataPackage.findResource(TABLE_OCCURRENCE).isPresent()) {
      log.info("Building occurrence-core ExtendedRecords");
      records = buildOccurrenceCoreDataset(spark, dataPackage, parquetBasePath);
    } else {
      log.warn(
          "Dataset {} has no event or occurrence table in datapackage.json; writing empty verbatim",
          datasetId);
      records = spark.emptyDataset(Encoders.bean(ExtendedRecord.class));
    }

    records.write().mode(SaveMode.Overwrite).format("avro").save(verbatimOutputPath);

    log.info(
        "DwcDpVerbatimConverter completed for dataset {} attempt {} in {}ms",
        datasetId,
        attempt,
        System.currentTimeMillis() - start);
  }

  // ---- event-core path ----

  /**
   * Builds a distributed Dataset of event-core ExtendedRecords.
   *
   * <p>Occurrences (if present) are joined by eventID and attached as the Occurrence extension.
   * Media (if present) is joined via event-media and attached as the Multimedia extension.
   *
   * <p>Extensions are serialised to JSON strings so they can cross executor boundaries, then
   * deserialised back when assembling the ExtendedRecord in the map function.
   */
  static Dataset<ExtendedRecord> buildEventCoreDataset(
      SparkSession spark, DataPackage dataPackage, String basePath) {

    DataPackageResource eventResource = dataPackage.findResource(TABLE_EVENT).orElseThrow();
    Dataset<Row> eventDf = spark.read().parquet(basePath + "/" + eventResource.getPath());

    Optional<Dataset<Row>> occurrenceExtDf =
        buildOccurrenceExtensionDf(spark, dataPackage, basePath);
    Optional<Dataset<Row>> mediaExtDf = buildEventMediaExtensionDf(spark, dataPackage, basePath);

    Dataset<Row> joined = eventDf;
    if (occurrenceExtDf.isPresent()) {
      joined =
          joined
              .join(
                  occurrenceExtDf.get(),
                  joined.col("eventID").equalTo(occurrenceExtDf.get().col("eventID")),
                  "left_outer")
              .drop(occurrenceExtDf.get().col("eventID"));
    }
    if (mediaExtDf.isPresent()) {
      joined =
          joined
              .join(
                  mediaExtDf.get(),
                  joined.col("eventID").equalTo(mediaExtDf.get().col("eventID")),
                  "left_outer")
              .drop(mediaExtDf.get().col("eventID"));
    }

    final String[] eventColumns = eventDf.columns();
    final boolean hasOccExt = occurrenceExtDf.isPresent();
    final boolean hasMediaExt = mediaExtDf.isPresent();

    return joined
        .map(
            (MapFunction<Row, ExtendedRecord>)
                row -> {
                  String eventId = safeGetString(row, "eventID");
                  if (eventId == null || eventId.isEmpty()) {
                    return null;
                  }

                  Map<String, String> coreTerms = rowToTermMap(row, eventColumns);
                  Map<String, List<Map<String, String>>> extensions = new HashMap<>();

                  if (hasOccExt) {
                    String occJson = safeGetString(row, "occurrenceExtJson");
                    if (occJson != null) {
                      extensions.put(ROW_TYPE_OCCURRENCE, jsonToTermMapList(occJson));
                    }
                  }
                  if (hasMediaExt) {
                    String mediaJson = safeGetString(row, "mediaExtJson");
                    if (mediaJson != null) {
                      extensions.put(ROW_TYPE_MULTIMEDIA, jsonToTermMapList(mediaJson));
                    }
                  }

                  return ExtendedRecord.newBuilder()
                      .setId(eventId)
                      .setCoreId(eventId)
                      .setCoreRowType(CORE_ROW_TYPE_EVENT)
                      .setCoreTerms(coreTerms)
                      .setExtensions(extensions)
                      .build();
                },
            Encoders.bean(ExtendedRecord.class))
        .filter((FilterFunction<ExtendedRecord>) r -> r != null);
  }

  // ---- occurrence-core path ----

  static Dataset<ExtendedRecord> buildOccurrenceCoreDataset(
      SparkSession spark, DataPackage dataPackage, String basePath) {

    DataPackageResource occResource = dataPackage.findResource(TABLE_OCCURRENCE).orElseThrow();
    Dataset<Row> occurrenceDf = spark.read().parquet(basePath + "/" + occResource.getPath());

    Optional<Dataset<Row>> mediaExtDf =
        buildOccurrenceMediaExtensionDf(spark, dataPackage, basePath);

    Dataset<Row> joined = occurrenceDf;
    if (mediaExtDf.isPresent()) {
      joined =
          joined
              .join(
                  mediaExtDf.get(),
                  occurrenceDf.col("occurrenceID").equalTo(mediaExtDf.get().col("occurrenceID")),
                  "left_outer")
              .drop(mediaExtDf.get().col("occurrenceID"));
    }

    final String[] occColumns = occurrenceDf.columns();
    final boolean hasMediaExt = mediaExtDf.isPresent();

    return joined
        .map(
            (MapFunction<Row, ExtendedRecord>)
                row -> {
                  String occurrenceId = safeGetString(row, "occurrenceID");
                  if (occurrenceId == null || occurrenceId.isEmpty()) {
                    return null;
                  }

                  Map<String, String> coreTerms = rowToTermMap(row, occColumns);
                  Map<String, List<Map<String, String>>> extensions = new HashMap<>();

                  if (hasMediaExt) {
                    String mediaJson = safeGetString(row, "mediaExtJson");
                    if (mediaJson != null) {
                      extensions.put(ROW_TYPE_MULTIMEDIA, jsonToTermMapList(mediaJson));
                    }
                  }

                  return ExtendedRecord.newBuilder()
                      .setId(occurrenceId)
                      .setCoreId(occurrenceId)
                      .setCoreRowType(CORE_ROW_TYPE_OCCURRENCE)
                      .setCoreTerms(coreTerms)
                      .setExtensions(extensions)
                      .build();
                },
            Encoders.bean(ExtendedRecord.class))
        .filter((FilterFunction<ExtendedRecord>) r -> r != null);
  }

  // ---- extension Dataset builders ----

  /**
   * Returns a two-column Dataset (eventID, occurrenceExtJson) where occurrenceExtJson is a
   * JSON-serialised list of term maps grouped by eventID. Empty if the occurrence table is absent
   * or has no eventID column.
   */
  static Optional<Dataset<Row>> buildOccurrenceExtensionDf(
      SparkSession spark, DataPackage dataPackage, String basePath) {

    Optional<DataPackageResource> resource = dataPackage.findResource(TABLE_OCCURRENCE);
    if (resource.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> df = spark.read().parquet(basePath + "/" + resource.get().getPath());
    if (!Arrays.asList(df.columns()).contains("eventID")) {
      log.warn("occurrence table has no eventID column; skipping occurrence extension");
      return Optional.empty();
    }

    return Optional.of(
        aggregateAsJsonByKey(spark, df, df.columns(), "eventID", "occurrenceExtJson"));
  }

  /**
   * Returns a two-column Dataset (eventID, mediaExtJson) built by joining event-media with media,
   * grouped by eventID. Empty if either table is absent.
   */
  static Optional<Dataset<Row>> buildEventMediaExtensionDf(
      SparkSession spark, DataPackage dataPackage, String basePath) {

    Optional<DataPackageResource> eventMediaResource = dataPackage.findResource(TABLE_EVENT_MEDIA);
    Optional<DataPackageResource> mediaResource = dataPackage.findResource(TABLE_MEDIA);
    if (eventMediaResource.isEmpty() || mediaResource.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> eventMediaDf =
        spark.read().parquet(basePath + "/" + eventMediaResource.get().getPath());
    Dataset<Row> mediaDf = spark.read().parquet(basePath + "/" + mediaResource.get().getPath());

    Dataset<Row> joined =
        eventMediaDf
            .join(mediaDf, eventMediaDf.col("mediaID").equalTo(mediaDf.col("mediaID")))
            .drop(mediaDf.col("mediaID"));

    return Optional.of(
        aggregateAsJsonByKey(spark, joined, joined.columns(), "eventID", "mediaExtJson"));
  }

  /**
   * Returns a two-column Dataset (occurrenceID, mediaExtJson) built by joining occurrence-media
   * with media, grouped by occurrenceID. Empty if either table is absent.
   */
  static Optional<Dataset<Row>> buildOccurrenceMediaExtensionDf(
      SparkSession spark, DataPackage dataPackage, String basePath) {

    Optional<DataPackageResource> occMediaResource =
        dataPackage.findResource(TABLE_OCCURRENCE_MEDIA);
    Optional<DataPackageResource> mediaResource = dataPackage.findResource(TABLE_MEDIA);
    if (occMediaResource.isEmpty() || mediaResource.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> occMediaDf =
        spark.read().parquet(basePath + "/" + occMediaResource.get().getPath());
    Dataset<Row> mediaDf = spark.read().parquet(basePath + "/" + mediaResource.get().getPath());

    Dataset<Row> joined =
        occMediaDf
            .join(mediaDf, occMediaDf.col("mediaID").equalTo(mediaDf.col("mediaID")))
            .drop(mediaDf.col("mediaID"));

    return Optional.of(
        aggregateAsJsonByKey(spark, joined, joined.columns(), "occurrenceID", "mediaExtJson"));
  }

  /**
   * Groups a dataset by {@code keyColumn}, serialises all rows per key into a JSON array of term
   * maps, and returns a two-column Dataset: (keyColumn, jsonColumnName).
   */
  private static Dataset<Row> aggregateAsJsonByKey(
      SparkSession spark,
      Dataset<Row> df,
      String[] allColumns,
      String keyColumn,
      String jsonColumnName) {

    return df.groupByKey(
            (MapFunction<Row, String>) row -> safeGetString(row, keyColumn), Encoders.STRING())
        .mapGroups(
            (MapGroupsFunction<String, Row, Row>)
                (key, iter) -> {
                  List<Map<String, String>> termMaps = new ArrayList<>();
                  while (iter.hasNext()) {
                    termMaps.add(rowToTermMap(iter.next(), allColumns));
                  }
                  return RowFactory.create(key, MAPPER.writeValueAsString(termMaps));
                },
            Encoders.row(
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField(keyColumn, DataTypes.StringType, true),
                      DataTypes.createStructField(jsonColumnName, DataTypes.StringType, true)
                    })));
  }

  // ---- term mapping ----

  /**
   * Converts a Spark Row into a map of qualified term URI -> string value. Null values are omitted.
   * Columns that TermFactory cannot resolve are kept under their raw column name so no
   * domain-specific data is silently lost.
   */
  static Map<String, String> rowToTermMap(Row row, String[] columns) {
    Map<String, String> terms = new HashMap<>();
    for (String col : columns) {
      String value = safeGetString(row, col);
      if (value != null) {
        terms.put(resolveTermUri(col), value);
      }
    }
    return terms;
  }

  /**
   * Resolves a DwC-DP column name to a qualified term URI via TermFactory. Falls back to the raw
   * column name for domain-specific terms TermFactory cannot resolve.
   */
  static String resolveTermUri(String columnName) {
    try {
      Term term = TERM_FACTORY.findTerm(columnName);
      if (term != null && !(term instanceof org.gbif.dwc.terms.UnknownTerm)) {
        return term.qualifiedName();
      }
    } catch (Exception e) {
      log.debug("TermFactory could not resolve '{}', keeping raw name", columnName);
    }
    return columnName;
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, String>> jsonToTermMapList(String json) throws IOException {
    return MAPPER.readValue(json, List.class);
  }

  private static String safeGetString(Row row, String fieldName) {
    try {
      int idx = row.fieldIndex(fieldName);
      if (row.isNullAt(idx)) {
        return null;
      }
      Object value = row.get(idx);
      return value == null ? null : value.toString();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  // ---- datapackage.json model ----

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DataPackage {
    private List<DataPackageResource> resources = new ArrayList<>();

    public Optional<DataPackageResource> findResource(String name) {
      return resources.stream().filter(r -> name.equals(r.getName())).findFirst();
    }
  }

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DataPackageResource {
    private String name;
    private String path;
    private DataPackageSchema schema;
  }

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DataPackageSchema {
    private List<DataPackageField> fields = new ArrayList<>();
  }

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DataPackageField {
    private String name;
    private String type;
  }
}
