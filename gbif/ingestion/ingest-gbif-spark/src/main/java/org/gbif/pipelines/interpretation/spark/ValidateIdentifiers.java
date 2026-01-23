package org.gbif.pipelines.interpretation.spark;

import static org.apache.spark.sql.functions.sum;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.*;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_ABSENT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_INVALID;
import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;
import static org.gbif.pipelines.interpretation.Metrics.*;
import static org.gbif.pipelines.interpretation.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.interpretation.spark.Directories.*;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getSparkSession;
import static org.gbif.pipelines.interpretation.standalone.DistributedUtil.timeAndRecPerSecond;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.OccurrenceExtensionConverter;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.interpretation.transform.GbifIdTransform;
import org.gbif.pipelines.interpretation.transform.utils.KeygenServiceFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.slf4j.MDC;

/**
 * Validates the identifiers in a dataset of ExtendedRecords and generates IdentifierRecords. This
 * is the VERBATIM_TO_IDENTIFIER step in the data pipelines.
 *
 * <p>This pipeline performs the following steps:
 *
 * <ol>
 *   <li>Reads ExtendedRecords from an Avro file.
 *   <li>Checks for occurrence extensions and converts them to individual ExtendedRecords if
 *       necessary.
 *   <li>Validates the identifiers by checking for duplicate IDs and counting unique IDs.
 *   <li>Transforms ExtendedRecords to IdentifierRecords using the GbifIdTransform.
 *   <li>Writes the IdentifierRecords to a Parquet file.
 * </ol>
 */
@Slf4j
public class ValidateIdentifiers {

  public static final String METRICS_FILENAME = "verbatim-to-identifier.yml";

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(
        names = "--tripletValid",
        description = "DWCA validation from crawler, all triplets are unique",
        required = false,
        arity = 1)
    private boolean tripletValid = false;

    @Parameter(
        names = "--occurrenceIdValid",
        description = "DWCA validation from crawler, all occurrenceIds are unique",
        required = false,
        arity = 1)
    private boolean occurrenceIdValid = true;

    @Parameter(
        names = "--useExtendedRecordId",
        description = "Skips gbif id generation and copies ids from ExtendedRecord ids",
        required = false,
        arity = 1)
    private boolean useExtendedRecordId = false;

    @Parameter(names = "--numberOfShards", description = "Number of shards", required = false)
    private int numberOfShards;

    @Parameter(
        names = "--config",
        description = "Path to YAML configuration file",
        required = false)
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = "--master",
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    boolean help;
  }

  public static void main(String[] argsv) throws Exception {

    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true); // to ease airflow/registry integration
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(args.master, args.appName, config, ValidateIdentifiers::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);
    /* ############ standard init block - end ########## */

    String datasetID = args.datasetId;
    int attempt = args.attempt;
    runValidation(
        spark,
        fileSystem,
        config,
        datasetID,
        attempt,
        args.numberOfShards,
        args.tripletValid,
        args.occurrenceIdValid,
        args.useExtendedRecordId);

    spark.stop();
    spark.close();
    fileSystem.close();
    System.exit(0);
  }

  public static void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    sparkBuilder.config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1");
  }

  public static void runValidation(
      SparkSession spark,
      FileSystem fs,
      PipelinesConfig config,
      String datasetID,
      int attempt,
      int numberOfShards,
      boolean tripletValid,
      boolean occurrenceIdValid,
      boolean useExtendedRecordId)
      throws Exception {

    try {

      long start = System.currentTimeMillis();
      MDC.put("datasetKey", datasetID);
      log.info(
          "Starting validation with tripleValid: {}, occurrenceIdValid: {}, useExtendedRecordId: {}",
          tripletValid,
          occurrenceIdValid,
          useExtendedRecordId);

      String inputPath = config.getInputPath() + "/" + datasetID + "/" + attempt;
      String outputPath = config.getOutputPath() + "/" + datasetID + "/" + attempt;

      // Read the verbatim input
      Dataset<ExtendedRecord> records =
          spark
              .read()
              .format("avro")
              .load(inputPath + "/verbatim.avro")
              .repartition(numberOfShards)
              .as(Encoders.bean(ExtendedRecord.class));

      // read the extended records and check for occurrences in the extensions
      Dataset<ExtendedRecord> recordsExpanded =
          checkExtensionsForOccurrence(spark, records, outputPath);

      // collect metrics
      Map<String, Long> metrics = new HashMap<>();

      // validate the identifiers from the extended records
      validateIdentifiers(recordsExpanded, metrics);

      // run the identifier transform - note: this does not generate new gbifIds
      identifierTransform(
              config,
              datasetID,
              tripletValid,
              occurrenceIdValid,
              useExtendedRecordId,
              recordsExpanded)
          .repartition(numberOfShards)
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(outputPath + "/" + IDENTIFIERS_TRANSFORMED);

      // reload
      Dataset<IdentifierRecord> identifiers =
          spark
              .read()
              .format("parquet")
              .load(outputPath + "/" + IDENTIFIERS_TRANSFORMED)
              .as(Encoders.bean(IdentifierRecord.class));

      // get the records that are valid and persisted - i.e. if the have an internalId (gbifId)
      Dataset<IdentifierRecord> validIdentifiers = validAndPersisted(identifiers, metrics);

      // get the absent records - i.e. records not assigned a gbifId and not stored in hbase
      Dataset<IdentifierRecord> absentIdentifiers = absentAndFilteredCount(identifiers, metrics);

      // get the invalid records - i.e. records with an invalid gbifId
      Dataset<IdentifierRecord> invalidIdentifiers = invalid(absentIdentifiers, metrics);

      // 1. write unique ids
      validIdentifiers
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(outputPath + "/" + IDENTIFIERS_VALID);

      // 2. write invalid ids
      invalidIdentifiers
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(outputPath + "/identifiers_invalid");

      // 3. write absent ids
      absentIdentifiers
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(outputPath + "/" + IDENTIFIERS_ABSENT);

      // 4. write metrics to yaml
      writeMetricsYaml(fs, metrics, outputPath + "/" + METRICS_FILENAME);

      // clean up
      fs.delete(new Path(outputPath + "/" + IDENTIFIERS_TRANSFORMED), true);

      log.info(
          timeAndRecPerSecond(
              "identifiers", start, metrics.get(VALID_GBIF_ID_COUNT + "Attempted")));

    } finally {
      MDC.clear();
    }
  }

  /**
   * Computes the count of records with absent GBIF IDs and the count of records with filtered GBIF
   * IDs.
   *
   * @param identifiers Dataset of IdentifierRecord to analyze
   * @return Map with counts of absent and filtered GBIF IDs
   */
  public static Dataset<IdentifierRecord> absentAndFilteredCount(
      Dataset<IdentifierRecord> identifiers, Map<String, Long> metrics) {

    Dataset<IdentifierRecord> absentRecords =
        identifiers.filter(
            new FilterFunction<IdentifierRecord>() {
              @Override
              public boolean call(IdentifierRecord ir) throws Exception {
                return ir.getInternalId() == null
                    && ir.getIssues().getIssueList().contains(GBIF_ID_ABSENT);
              }
            });

    Long absentCount = absentRecords.count();
    Long identifiersCount = identifiers.count();

    metrics.put(ABSENT_GBIF_ID_COUNT + "Attempted", absentRecords.count());
    metrics.put(FILTERED_GBIF_IDS_COUNT + "Attempted", identifiersCount - absentCount);

    return absentRecords;
  }

  public static Dataset<IdentifierRecord> validAndPersisted(
      Dataset<IdentifierRecord> identifiers, Map<String, Long> metrics) {

    Dataset<IdentifierRecord> validRecords =
        identifiers.filter(
            new FilterFunction<IdentifierRecord>() {
              @Override
              public boolean call(IdentifierRecord ir) throws Exception {
                return ir.getInternalId() != null
                    && !ir.getIssues().getIssueList().contains(GBIF_ID_INVALID)
                    && !ir.getIssues().getIssueList().contains(GBIF_ID_ABSENT);
              }
            });

    metrics.put(VALID_GBIF_ID_COUNT + "Attempted", validRecords.count());
    return validRecords;
  }

  public static Dataset<IdentifierRecord> invalid(
      Dataset<IdentifierRecord> identifiers, Map<String, Long> metrics) {

    Dataset<IdentifierRecord> invalidRecords =
        identifiers.filter(
            new FilterFunction<IdentifierRecord>() {
              @Override
              public boolean call(IdentifierRecord ir) throws Exception {
                return ir.getInternalId() == null
                    && ir.getIssues().getIssueList().contains(GBIF_ID_INVALID);
              }
            });
    metrics.put(INVALID_GBIF_ID_COUNT + "Attempted", invalidRecords.count());
    return invalidRecords;
  }

  /**
   * Validates the identifiers in the given ExtendedRecord dataset.
   *
   * <p>It checks for duplicate IDs and computes the count of unique (non-duplicate) IDs.
   *
   * @param records Dataset of ExtendedRecord to validate
   * @return Map with counts of duplicate and unique IDs
   */
  public static void validateIdentifiers(
      Dataset<ExtendedRecord> records, Map<String, Long> metrics) {

    // Find duplicate ID counts
    Dataset<Row> duplicates = records.groupBy("id").count().filter("count > 1");

    long duplicateCount =
        duplicates.isEmpty() ? 0L : duplicates.agg(sum("count")).first().getLong(0);

    metrics.put(DUPLICATE_IDS_COUNT + "Attempted", duplicateCount);

    // Compute unique (non-duplicate) IDs
    long totalCount = records.count();
    metrics.put(UNIQUE_IDS_COUNT + "Attempted", totalCount - duplicateCount);
    metrics.put(GBIF_ID_RECORDS_COUNT + "Attempted", totalCount);
  }

  /**
   * This method checks if the ExtendedRecord dataset contains occurrence extensions. If so, it
   * converts them to individual ExtendedRecords. If the core type is Occurrence, it returns the
   * records as they are. If neither condition is met, it logs a warning and returns an empty
   * dataset.
   *
   * @param records Dataset of ExtendedRecord to check and convert
   * @return Dataset of ExtendedRecord containing only occurrence records
   */
  public static Dataset<ExtendedRecord> checkExtensionsForOccurrence(
      SparkSession spark, Dataset<ExtendedRecord> records, String outputPath) {

    Dataset<ExtendedRecord> expanded =
        records.flatMap(
            (FlatMapFunction<ExtendedRecord, ExtendedRecord>)
                er -> {
                  String occurrenceQName = DwcTerm.Occurrence.qualifiedName();
                  Map<String, List<Map<String, String>>> extensions = er.getExtensions();
                  List<Map<String, String>> occurrenceExts =
                      extensions != null ? extensions.get(occurrenceQName) : null;

                  // Case 1: Event/Taxon record with occurrence extensions
                  if (occurrenceExts != null) {
                    if (occurrenceExts.isEmpty()) {
                      // Empty extensions — expected for some archives (see issue #471)
                      log.debug(
                          "Event/Taxon core archive with empty occurrence extensions for record [{}]",
                          er.getId());
                      return Collections.<ExtendedRecord>emptyIterator();
                    }
                    // Convert and return occurrence extension records
                    List<ExtendedRecord> erOcc = OccurrenceExtensionConverter.convert(er);
                    return erOcc.iterator();
                  }

                  // Case 2: Core type is Occurrence
                  if (occurrenceQName.equals(er.getCoreRowType())) {
                    return Collections.singletonList(er).iterator();
                  }

                  // Case 3: Unexpected — no occurrence extensions, not an occurrence core
                  log.warn(
                      "Record [{}] has no occurrence extensions and is not an Occurrence core. Possible data inconsistency.",
                      er.getId());
                  return Collections.<ExtendedRecord>emptyIterator();
                },
            Encoders.bean(ExtendedRecord.class));

    // write out the expanded records for debugging/inspection
    expanded.write().mode(SaveMode.Overwrite).parquet(outputPath + "/" + OCCURRENCE_VERBATIM);

    // reload
    return spark
        .read()
        .format("parquet")
        .load(outputPath + "/" + OCCURRENCE_VERBATIM)
        .as(Encoders.bean(ExtendedRecord.class));
  }

  /**
   * Transforms ExtendedRecord dataset to IdentifierRecord dataset.
   *
   * @param config Pipelines configuration
   * @param datasetId Dataset identifier
   * @param isTripletValid
   * @param isOccurrenceIdValid
   * @param isUseExtendedRecordId
   * @param records Dataset of ExtendedRecord to transform
   * @return Dataset of IdentifierRecord
   */
  public static Dataset<IdentifierRecord> identifierTransform(
      final PipelinesConfig config,
      final String datasetId,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean isUseExtendedRecordId,
      Dataset<ExtendedRecord> records) {

    GbifIdTransform transform =
        GbifIdTransform.builder()
            .generateIdIfAbsent(false)
            .isTripletValid(isTripletValid)
            .isOccurrenceIdValid(isOccurrenceIdValid)
            .useExtendedRecordId(isUseExtendedRecordId)
            .keygenServiceSupplier(
                (SerializableSupplier<HBaseLockingKey>)
                    () -> KeygenServiceFactory.create(config, datasetId))
            .build();

    return records.map(
        (MapFunction<ExtendedRecord, IdentifierRecord>)
            er -> {
              return transform.convert(er).get();
            },
        Encoders.bean(IdentifierRecord.class));
  }
}
