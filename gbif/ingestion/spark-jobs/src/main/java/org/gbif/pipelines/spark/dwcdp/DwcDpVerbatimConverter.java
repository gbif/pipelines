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
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.MetricsUtil;
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
  public static final String TABLE_EVENT_ASSERTION = "event-assertion";
  public static final String TABLE_OCCURRENCE_ASSERTION = "occurrence-assertion";
  public static final String TABLE_PROTOCOL = "protocol";
  public static final String TABLE_SURVEY = "survey";
  public static final String TABLE_SURVEY_SURVEY_TARGET = "survey-survey-target";
  public static final String TABLE_SURVEY_TARGET = "survey-target";

  // Extension row type URI for multimedia
  public static final String ROW_TYPE_MULTIMEDIA = "http://rs.tdwg.org/ac/terms/Multimedia";

  // Extension row type URIs for MeasurementOrFact and ExtendedMeasurementOrFact
  public static final String ROW_TYPE_MEASUREMENT_OR_FACT =
      "http://rs.tdwg.org/dwc/terms/MeasurementOrFact";
  public static final String ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT =
      "http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact";

  // Extension row type URI for the Humboldt Ecological Inventory Extension
  public static final String ROW_TYPE_HUMBOLDT = "http://rs.tdwg.org/eco/terms/Event";

  // Core row type URIs
  public static final String CORE_ROW_TYPE_EVENT = DwcTerm.Event.qualifiedName();
  public static final String CORE_ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  // Extension row type for occurrences attached to an event core
  public static final String ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  private static final org.apache.avro.Schema EXTENDED_RECORD_SCHEMA = loadExtendedRecordSchema();
  private static final TermFactory TERM_FACTORY = TermFactory.instance();
  public static final String AVRO_EXTENDED_RECORD_AVSC = "avro/extended-record.avsc";

  /**
   * Returns the ExtendedRecord Avro schema JSON string, used by Spark's avroSchema write option.
   */
  static String extendedRecordSchemaJson() {
    return EXTENDED_RECORD_SCHEMA.toString();
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

  private static final ObjectMapper MAPPER = MapperUtil.MAPPER;

  // DwC-DP assertion column names → DwC-A eMoF term names.
  // The FK columns (event_fk / occurrence_fk) are handled separately in remapAssertionColumns.
  // assertionProtocol_fk → measurementMethod is lossy: the FK value (an ID string) is stored as
  // free text because a protocol lookup table is not available at this stage.
  private static final Map<String, String> ASSERTION_TO_EMOF_COLUMNS =
      Map.ofEntries(
          Map.entry("assertionID", "measurementID"),
          Map.entry("assertionType", "measurementType"),
          Map.entry("assertionTypeIRI", "measurementTypeID"),
          Map.entry("assertionValue", "measurementValue"),
          Map.entry("assertionValueIRI", "measurementValueID"),
          Map.entry("assertionUnit", "measurementUnit"),
          Map.entry("assertionUnitIRI", "measurementUnitID"),
          Map.entry("assertionError", "measurementAccuracy"),
          Map.entry("assertionBy", "measurementDeterminedBy"),
          Map.entry("assertionMadeDate", "measurementDeterminedDate"),
          Map.entry("assertionRemarks", "measurementRemarks"),
          Map.entry("assertionProtocol_fk", "measurementMethod"));

  /**
   * Joins an assertion Dataset against its parent entity table to replace the internal FK ({@code
   * event_fk} or {@code occurrence_fk}) with the natural DwC identifier ({@code eventID} or {@code
   * occurrenceID}). Optionally joins the {@code protocol} table — if present — to resolve {@code
   * assertionProtocol_fk} into a human-readable {@code measurementMethod} string using {@code
   * protocolDescription}; when the protocol table is absent the raw FK value is kept under {@code
   * assertionProtocol_fk} and {@link #remapAssertionColumns} will rename it to {@code
   * measurementMethod} as a fallback.
   */
  private static Dataset<Row> resolveAssertionLinks(
      SparkSession spark,
      DataPackage dataPackage,
      String basePath,
      Dataset<Row> assertionDf,
      String fkColumn,
      Dataset<Row> parentDf,
      String parentPkColumn,
      String parentIdColumn) {

    Dataset<Row> result =
        assertionDf
            .join(
                parentDf.select(parentPkColumn, parentIdColumn),
                assertionDf.col(fkColumn).equalTo(parentDf.col(parentPkColumn)),
                "left_outer")
            .drop(parentDf.col(parentPkColumn))
            .drop(assertionDf.col(fkColumn));

    Optional<DataPackageResource> protocolResource = dataPackage.findResource(TABLE_PROTOCOL);
    if (protocolResource.isPresent()
        && Arrays.asList(result.columns()).contains("assertionProtocol_fk")) {
      Dataset<Row> protocolDf =
          spark
              .read()
              .parquet(basePath + "/" + protocolResource.get().getPath())
              .select("protocol_pk", "protocolDescription");
      result =
          result
              .join(
                  protocolDf,
                  result.col("assertionProtocol_fk").equalTo(protocolDf.col("protocol_pk")),
                  "left_outer")
              .drop(protocolDf.col("protocol_pk"))
              .drop("assertionProtocol_fk")
              .withColumnRenamed("protocolDescription", "measurementMethod");
    }

    return result;
  }

  /** Renames DwC-DP assertion column names to their DwC-A eMoF equivalents. */
  private static Dataset<Row> remapAssertionColumns(Dataset<Row> df) {
    Dataset<Row> result = df;
    for (Map.Entry<String, String> e : ASSERTION_TO_EMOF_COLUMNS.entrySet()) {
      if (Arrays.asList(result.columns()).contains(e.getKey())) {
        result = result.withColumnRenamed(e.getKey(), e.getValue());
      }
    }
    return result;
  }

  private DwcDpVerbatimConverter() {}

  /**
   * Result of {@link #convert}, carrying the record counts gathered while building and writing
   * verbatim.avro. Used by {@code DwcDpToVerbatimCallback} to populate the outgoing message's
   * {@code validationResult} / event and occurrence counts, so the next step's balancer handler has
   * accurate routing information without re-reading HDFS.
   */
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

    // Write to a temporary directory to avoid naming conflict with the final single-file target.
    // Spark always writes a directory of part files; we then rename the single part file to
    // the final path so verbatim.avro is a literal file, matching the DwC-A pipeline convention.
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
   * Writes {@code archive-to-verbatim.yml} to the attempt directory under the configured input
   * path, in the same format and encoding as {@code DwcaToAvroPipeline} so that {@code
   * VerbatimMessageHandler} in the balancer can read the record counts via {@link
   * HdfsUtils#getLongByKey}.
   *
   * <p>{@code archiveToErCount} is always 0 for DwC-DP — the ExtendedRecord/verbatim distinction
   * from DwC-A doesn't apply here. {@code archiveToOccurrenceCount} is the total number of
   * occurrence rows in the data package (core or extension). {@code eventCoreRecordsCount} is the
   * total number of event rows (0 if no event table). {@code archiveToLargestFileCount} is the row
   * count of the largest table present, used by the balancer as a fallback signal for runner sizing
   * on the next step.
   *
   * <p>Resulting layout under the attempt directory:
   *
   * <pre>{@code
   * {inputPath}/{datasetId}/{attempt}/
   * ├── verbatim.avro/
   * │   ├── part-00000-....avro
   * │   └── ...
   * └── archive-to-verbatim.yml   <-- written here
   * }</pre>
   *
   * @return the same counts written to the file, for the caller to forward in outgoing messages
   */
  static VerbatimConversionMetrics writeMetrics(
      SparkSession spark,
      DataPackage dataPackage,
      String datasetBasePath,
      FileSystem fileSystem,
      String datasetId) {

    long occurrenceCount =
        dataPackage
            .findResource(TABLE_OCCURRENCE)
            .map(r -> countResourceRows(spark, datasetBasePath, r))
            .orElse(0L);

    long eventCount =
        dataPackage
            .findResource(TABLE_EVENT)
            .map(r -> countResourceRows(spark, datasetBasePath, r))
            .orElse(0L);

    long largestFileCount =
        dataPackage.getResources().stream()
            .mapToLong(r -> countResourceRows(spark, datasetBasePath, r))
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

  /**
   * Renames the single {@code .avro} part file produced by {@code coalesce(1)} from the temporary
   * parts directory to the final target path as a literal file, then deletes the temporary
   * directory ({@code _SUCCESS}, {@code .crc} files, etc.).
   *
   * <pre>{@code
   * Before:
   *   verbatim.avro.parts/
   *     part-00000-xxxx.avro
   *     _SUCCESS
   *
   * After:
   *   verbatim.avro           <-- literal file, not a directory
   * }</pre>
   */
  static void mergeToSingleFile(FileSystem fileSystem, String tempPath, String targetPath)
      throws IOException {
    org.apache.hadoop.fs.Path temp = new org.apache.hadoop.fs.Path(tempPath);
    org.apache.hadoop.fs.Path target = new org.apache.hadoop.fs.Path(targetPath);

    // Delete any pre-existing target file from a previous attempt
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

  private static long countResourceRows(
      SparkSession spark, String datasetBasePath, DataPackageResource resource) {
    return spark.read().parquet(datasetBasePath + "/" + resource.getPath()).count();
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
    Optional<Dataset<Row>> assertionExtDf =
        buildEventAssertionExtensionDf(spark, dataPackage, basePath);
    Optional<Dataset<Row>> humboldtExtDf =
        buildHumboldtExtensionDf(spark, dataPackage, basePath);

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
    if (assertionExtDf.isPresent()) {
      joined =
          joined
              .join(
                  assertionExtDf.get(),
                  joined.col("eventID").equalTo(assertionExtDf.get().col("eventID")),
                  "left_outer")
              .drop(assertionExtDf.get().col("eventID"));
    }
    if (humboldtExtDf.isPresent()) {
      joined =
          joined
              .join(
                  humboldtExtDf.get(),
                  joined.col("eventID").equalTo(humboldtExtDf.get().col("eventID")),
                  "left_outer")
              .drop(humboldtExtDf.get().col("eventID"));
    }

    final String[] eventColumns = eventDf.columns();
    final boolean hasOccExt = occurrenceExtDf.isPresent();
    final boolean hasMediaExt = mediaExtDf.isPresent();
    final boolean hasAssertionExt = assertionExtDf.isPresent();
    final boolean hasHumboldtExt = humboldtExtDf.isPresent();

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
                  if (hasAssertionExt) {
                    String assertionJson = safeGetString(row, "assertionExtJson");
                    if (assertionJson != null) {
                      extensions.put(
                          ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT, jsonToTermMapList(assertionJson));
                    }
                  }
                  if (hasHumboldtExt) {
                    String humboldtJson = safeGetString(row, "humboldtExtJson");
                    if (humboldtJson != null) {
                      extensions.put(ROW_TYPE_HUMBOLDT, jsonToTermMapList(humboldtJson));
                    }
                  }

                  return ExtendedRecord.newBuilder()
                      .setId(eventId)
                      .setCoreId(null)
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
    Optional<Dataset<Row>> assertionExtDf =
        buildOccurrenceAssertionExtensionDf(spark, dataPackage, basePath);

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
    if (assertionExtDf.isPresent()) {
      joined =
          joined
              .join(
                  assertionExtDf.get(),
                  occurrenceDf
                      .col("occurrenceID")
                      .equalTo(assertionExtDf.get().col("occurrenceID")),
                  "left_outer")
              .drop(assertionExtDf.get().col("occurrenceID"));
    }

    final String[] occColumns = occurrenceDf.columns();
    final boolean hasMediaExt = mediaExtDf.isPresent();
    final boolean hasAssertionExt = assertionExtDf.isPresent();

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
                  if (hasAssertionExt) {
                    String assertionJson = safeGetString(row, "assertionExtJson");
                    if (assertionJson != null) {
                      extensions.put(
                          ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT, jsonToTermMapList(assertionJson));
                    }
                  }

                  return ExtendedRecord.newBuilder()
                      .setId(occurrenceId)
                      .setCoreId(null)
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
   * Returns a two-column Dataset {@code (eventID, assertionExtJson)} built from the {@code
   * event-assertion} table. {@code event_fk} is resolved to the natural {@code eventID} from the
   * {@code event} table; {@code assertionProtocol_fk} is resolved to {@code measurementMethod} via
   * the {@code protocol} table when available. Assertion column names are remapped to their DwC-A
   * eMoF equivalents before serialisation. Empty if the table is absent.
   */
  static Optional<Dataset<Row>> buildEventAssertionExtensionDf(
      SparkSession spark, DataPackage dataPackage, String basePath) {

    Optional<DataPackageResource> resource = dataPackage.findResource(TABLE_EVENT_ASSERTION);
    if (resource.isEmpty()) {
      return Optional.empty();
    }

    DataPackageResource eventResource = dataPackage.findResource(TABLE_EVENT).orElseThrow();
    Dataset<Row> assertionDf = spark.read().parquet(basePath + "/" + resource.get().getPath());
    Dataset<Row> eventDf = spark.read().parquet(basePath + "/" + eventResource.getPath());

    Dataset<Row> df =
        remapAssertionColumns(
            resolveAssertionLinks(
                spark,
                dataPackage,
                basePath,
                assertionDf,
                "event_fk",
                eventDf,
                "event_pk",
                "eventID"));
    return Optional.of(
        aggregateAsJsonByKey(spark, df, df.columns(), "eventID", "assertionExtJson"));
  }

  /**
   * Returns a two-column Dataset {@code (occurrenceID, assertionExtJson)} built from the {@code
   * occurrence-assertion} table. {@code occurrence_fk} is resolved to the natural {@code
   * occurrenceID} from the {@code occurrence} table; {@code assertionProtocol_fk} is resolved to
   * {@code measurementMethod} via the {@code protocol} table when available. Assertion column names
   * are remapped to their DwC-A eMoF equivalents before serialisation. Empty if the table is
   * absent.
   */
  static Optional<Dataset<Row>> buildOccurrenceAssertionExtensionDf(
      SparkSession spark, DataPackage dataPackage, String basePath) {

    Optional<DataPackageResource> resource = dataPackage.findResource(TABLE_OCCURRENCE_ASSERTION);
    if (resource.isEmpty()) {
      return Optional.empty();
    }

    DataPackageResource occResource = dataPackage.findResource(TABLE_OCCURRENCE).orElseThrow();
    Dataset<Row> assertionDf = spark.read().parquet(basePath + "/" + resource.get().getPath());
    Dataset<Row> occurrenceDf = spark.read().parquet(basePath + "/" + occResource.getPath());

    Dataset<Row> df =
        remapAssertionColumns(
            resolveAssertionLinks(
                spark,
                dataPackage,
                basePath,
                assertionDf,
                "occurrence_fk",
                occurrenceDf,
                "occurrence_pk",
                "occurrenceID"));
    return Optional.of(
        aggregateAsJsonByKey(spark, df, df.columns(), "occurrenceID", "assertionExtJson"));
  }

  /**
   * Returns a two-column Dataset {@code (eventID, humboldtExtJson)} for the Humboldt Ecological
   * Inventory Extension, built from the {@code survey} table.
   *
   * <p>Join strategy:
   *
   * <ol>
   *   <li>{@code survey.event_fk} is resolved to the natural {@code eventID} via the {@code event}
   *       table ({@code event_fk → event_pk → eventID}).
   *   <li>When both {@code survey-survey-target} and {@code survey-target} are present, the survey
   *       rows are fanned out — one output row per linked survey-target — so each survey-target's
   *       fields ({@code surveyTargetDescription}, etc.) appear as a separate Humboldt extension
   *       row under the same {@code eventID}. Surveys with no survey-target produce a single row.
   * </ol>
   *
   * <p>All internal PK/FK columns ({@code survey_pk}, {@code event_fk}, {@code survey_fk},
   * {@code surveyTarget_fk}, {@code surveyTarget_pk}, {@code samplingProtocol_fk},
   * {@code samplingEffortProtocol_fk}) are dropped before serialisation. Survey field names match
   * Humboldt Extension term names directly so no column renaming is needed. Empty if the {@code
   * survey} table is absent.
   */
  static Optional<Dataset<Row>> buildHumboldtExtensionDf(
      SparkSession spark, DataPackage dataPackage, String basePath) {

    Optional<DataPackageResource> surveyResource = dataPackage.findResource(TABLE_SURVEY);
    if (surveyResource.isEmpty()) {
      return Optional.empty();
    }

    DataPackageResource eventResource = dataPackage.findResource(TABLE_EVENT).orElseThrow();
    Dataset<Row> surveyDf = spark.read().parquet(basePath + "/" + surveyResource.get().getPath());
    Dataset<Row> eventDf = spark.read().parquet(basePath + "/" + eventResource.getPath());

    // Resolve event_fk → natural eventID; keep survey_pk for the survey-target join below
    Dataset<Row> df =
        surveyDf
            .join(
                eventDf.select("event_pk", "eventID"),
                surveyDf.col("event_fk").equalTo(eventDf.col("event_pk")),
                "left_outer")
            .drop(eventDf.col("event_pk"))
            .drop(surveyDf.col("event_fk"))
            .drop("samplingProtocol_fk")
            .drop("samplingEffortProtocol_fk");

    // Fan-out to survey-target rows via the junction table (1:many per survey)
    Optional<DataPackageResource> junctionResource =
        dataPackage.findResource(TABLE_SURVEY_SURVEY_TARGET);
    Optional<DataPackageResource> targetResource = dataPackage.findResource(TABLE_SURVEY_TARGET);

    if (junctionResource.isPresent() && targetResource.isPresent()) {
      Dataset<Row> junctionDf =
          spark.read().parquet(basePath + "/" + junctionResource.get().getPath());
      Dataset<Row> targetDf =
          spark.read().parquet(basePath + "/" + targetResource.get().getPath());

      Dataset<Row> targets =
          junctionDf
              .join(
                  targetDf,
                  junctionDf.col("surveyTarget_fk").equalTo(targetDf.col("surveyTarget_pk")),
                  "inner")
              .drop(targetDf.col("surveyTarget_pk"))
              .drop(junctionDf.col("surveyTarget_fk"));

      df =
          df.join(targets, df.col("survey_pk").equalTo(targets.col("survey_fk")), "left_outer")
              .drop(targets.col("survey_fk"));
    }

    df = df.drop("survey_pk");

    return Optional.of(aggregateAsJsonByKey(spark, df, df.columns(), "eventID", "humboldtExtJson"));
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
