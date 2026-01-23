/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.interpretation.spark;

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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.interpretation.transform.*;
import org.gbif.pipelines.interpretation.transform.utils.KeygenServiceFactory;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.slf4j.MDC;
import scala.Tuple2;

/**
 * Main class for the Spark pipeline that interprets occurrence records.
 *
 * <p>This pipeline reads extended records and identifiers from disk, processes them, and writes the
 * interpreted occurrences to two outputs:
 *
 * <ol>
 *   <li>A Parquet file containing the occurrences in a format suitable for indexing in
 *       Elasticsearch
 *   <li>A Parquet file containing the occurrences in a format suitable for HDFS view
 * </ol>
 */
@Slf4j
public class Interpretation {

  static final ObjectMapper MAPPER = new ObjectMapper();

  public static final String METRICS_FILENAME = "verbatim-to-occurrence.yml";

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
        names = "--config",
        description = "Path to YAML configuration file",
        required = false)
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = "--master",
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(names = "--numberOfShards", description = "Number of shards")
    private int numberOfShards = 10;

    @Parameter(
        names = "--useCheckpoints",
        description = "Use checkpoints where possible",
        arity = 1)
    private boolean useCheckpoints = true;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
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

    PipelinesConfig config = loadConfig(args.config);
    String datasetId = args.datasetId;
    int attempt = args.attempt;

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(args.master, args.appName, config, Interpretation::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);
    /* ############ standard init block - end ########## */

    runInterpretation(
        spark,
        fileSystem,
        config,
        datasetId,
        attempt,
        args.numberOfShards,
        args.tripletValid,
        args.occurrenceIdValid,
        args.useCheckpoints);

    fileSystem.close();
    spark.stop();
    spark.close();
    System.exit(0);
  }

  public static void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    sparkBuilder.config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1");
  }

  /**
   * Runs the interpretation pipeline.
   *
   * @param spark The Spark session
   * @param fs The Hadoop file system
   * @param config The pipelines configuration
   * @param datasetId The dataset ID
   * @param attempt The attempt number
   * @param numberOfShards The number of shards to repartition the data
   * @param tripletValid Whether all triplets are valid
   * @param occurrenceIdValid Whether all occurrence IDs are valid
   * @param useCheckpoints Whether to use checkpoints for intermediate datasets
   * @throws IOException If an I/O error occurs
   */
  public static void runInterpretation(
      SparkSession spark,
      FileSystem fs,
      PipelinesConfig config,
      String datasetId,
      int attempt,
      int numberOfShards,
      Boolean tripletValid,
      Boolean occurrenceIdValid,
      Boolean useCheckpoints)
      throws IOException {

    long start = System.currentTimeMillis();

    try {
      MDC.put("datasetKey", datasetId);
      log.info(
          "Starting interpretation with tripleValid: {}, occurrenceIdValid: {}",
          tripletValid,
          occurrenceIdValid);

      String inputPath = String.format("%s/%s/%d", config.getInputPath(), datasetId, attempt);
      String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

      // Load the extended records
      sparkLog(spark, "loadExtendedRecords", "Loading extended records", useCheckpoints);
      Dataset<ExtendedRecord> extendedRecords =
          loadExtendedRecords(spark, config, inputPath, outputPath, numberOfShards, useCheckpoints);

      // Process identifiers - persisting new identifiers
      sparkLog(spark, "processIdentifiers", "Processing identifiers", useCheckpoints);
      processIdentifiers(spark, fs, config, outputPath, datasetId, tripletValid, occurrenceIdValid);

      // load identifiers
      sparkLog(spark, "loadIdentifiers", "Loading identifiers", useCheckpoints);
      Dataset<IdentifierRecord> identifiers = loadIdentifiers(spark, outputPath);
      final long identifiersCount = identifiers.count();

      // check all identifier records have a valid internal ID
      sparkLog(spark, "checkIdentifiers", "Checking identifiers", useCheckpoints);
      checkIdentifiers(identifiers);

      // join extended records and identifiers
      sparkLog(
          spark, "joinRecordsAndIdentifiers", "Joining records and identifiers", useCheckpoints);
      Dataset<Occurrence> simpleRecords =
          joinRecordsAndIdentifiers(
              spark, extendedRecords, identifiers, outputPath, useCheckpoints);

      // a single call to the registry to get the dataset metadata
      final MetadataRecord metadata = getMetadataRecord(config, datasetId);

      // run all transforms
      sparkLog(spark, "runTransforms", "Running transforms", useCheckpoints);
      Dataset<Occurrence> interpreted =
          runTransforms(spark, config, simpleRecords, metadata, outputPath, useCheckpoints);

      // write parquet for elastic
      sparkLog(spark, "toJson", "Writing JSON output", useCheckpoints);
      toJson(interpreted, metadata)
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(outputPath + "/" + OCCURRENCE_JSON);

      // write parquet for hdfs view
      sparkLog(spark, "toHdfs", "Writing HDFS output", useCheckpoints);
      toHdfs(interpreted, metadata)
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(outputPath + "/" + OCCURRENCE_HDFS);

      // cleanup intermediate parquet outputs
      HdfsConfigs hdfsConfigs =
          HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());
      FsUtils.deleteIfExist(hdfsConfigs, outputPath + "/" + EXTENDED_IDENTIFIERS);

      // write metrics to yaml
      writeMetricsYaml(
          fs,
          Map.of(
              PipelinesVariables.Metrics.BASIC_RECORDS_COUNT, identifiersCount,
              PipelinesVariables.Metrics.UNIQUE_GBIF_IDS_COUNT, identifiersCount),
          outputPath + "/" + METRICS_FILENAME);

      MDC.put("datasetKey", datasetId);
      log.info(timeAndRecPerSecond("interpretation", start, identifiersCount));
    } finally {
      MDC.remove("datasetKey");
    }
  }

  private static void checkIdentifiers(Dataset<IdentifierRecord> identifiers) {

    Dataset<IdentifierRecord> missingId =
        identifiers.filter(
            new FilterFunction<IdentifierRecord>() {
              @Override
              public boolean call(IdentifierRecord identifierRecord) throws Exception {
                return identifierRecord.getInternalId() == null;
              }
            });

    long recordsWithEmptyInternalId = missingId.count();
    if (recordsWithEmptyInternalId > 0) {
      IdentifierRecord record = missingId.first();
      log.warn(
          "Records with empty internal "
              + IDENTIFIERS
              + ": "
              + recordsWithEmptyInternalId
              + ". "
              + "Example record: "
              + record.toString());
    }
  }

  /**
   * Joins the extended records with their corresponding identifiers and creates a simple occurrence
   * record containing only the id, verbatim and identifier fields.
   *
   * @param extendedRecords
   * @param identifiers
   * @param outputPath
   * @param spark
   * @return The dataset of simple occurrence records.
   */
  private static Dataset<Occurrence> joinRecordsAndIdentifiers(
      SparkSession spark,
      Dataset<ExtendedRecord> extendedRecords,
      Dataset<IdentifierRecord> identifiers,
      String outputPath,
      boolean useCheckpoints) {

    Dataset<Occurrence> occurrences =
        extendedRecords
            .as("extendedRecord")
            .joinWith(identifiers, extendedRecords.col("id").equalTo(identifiers.col("id")))
            .map(
                (MapFunction<Tuple2<ExtendedRecord, IdentifierRecord>, Occurrence>)
                    row -> {
                      ExtendedRecord er = row._1;
                      IdentifierRecord ir = row._2;
                      return Occurrence.builder()
                          .id(er.getId())
                          .coreId(er.getCoreId())
                          .internalId(ir.getInternalId())
                          .verbatim(MAPPER.writeValueAsString(er))
                          .identifier(MAPPER.writeValueAsString(ir))
                          .build();
                    },
                Encoders.bean(Occurrence.class))
            // only include records with ids
            .filter((FilterFunction<Occurrence>) occurrence -> occurrence.getInternalId() != null);

    if (useCheckpoints) {
      occurrences.write().mode(SaveMode.Overwrite).parquet(outputPath + "/" + EXTENDED_IDENTIFIERS);
      return spark
          .read()
          .parquet(outputPath + "/" + EXTENDED_IDENTIFIERS)
          .as(Encoders.bean(Occurrence.class));
    } else {
      return occurrences;
    }
  }

  /**
   * Retrieves the metadata for the dataset from the metadata service.
   *
   * @param config The pipelines configuration.
   * @param datasetId The dataset ID.
   * @return The metadata record for the dataset.
   */
  static MetadataRecord getMetadataRecord(PipelinesConfig config, String datasetId) {
    MetadataServiceClient metadataServiceClient =
        MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    final MetadataRecord metadata = MetadataRecord.newBuilder().setDatasetKey(datasetId).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(datasetId, metadata);
    return metadata;
  }

  /**
   * Runs all the transforms on the simple records to produce fully interpreted occurrence records.
   *
   * @param config The pipelines configuration.
   * @param simpleRecords The dataset of simple occurrence records.
   * @param metadata The metadata record for the dataset.
   * @return The dataset of fully interpreted occurrence records.
   */
  private static Dataset<Occurrence> runTransforms(
      SparkSession spark,
      PipelinesConfig config,
      Dataset<Occurrence> simpleRecords,
      MetadataRecord metadata,
      String outputPath,
      Boolean useCheckpoints) {

    // Set up our transforms
    DefaultValuesTransform defaultValuesTransform = DefaultValuesTransform.create(config, metadata);
    MultiTaxonomyTransform taxonomyTransform = MultiTaxonomyTransform.create(config);
    LocationTransform locationTransform = LocationTransform.create(config);
    GrscicollTransform grscicollTransform = GrscicollTransform.create(config);
    TemporalTransform temporalTransform = TemporalTransform.create(config);
    BasicTransform basicTransform = BasicTransform.create(config);
    DnDerivedDataTransform dnDerivedDataTransform = DnDerivedDataTransform.create();
    MultimediaTransform multimediaTransform = MultimediaTransform.create(config);
    ImageTransform imageTransform = ImageTransform.create(config);
    AudubonTransform audubonTransform = AudubonTransform.create(config);
    ClusteringTransform clusteringTransform = ClusteringTransform.create(config);

    // Loop over all records and interpret them
    Dataset<Occurrence> interpreted =
        simpleRecords.map(
            (MapFunction<Occurrence, Occurrence>)
                simpleRecord -> {
                  ExtendedRecord er =
                      MAPPER.readValue(simpleRecord.getVerbatim(), ExtendedRecord.class);
                  IdentifierRecord idr =
                      MAPPER.readValue(simpleRecord.getIdentifier(), IdentifierRecord.class);

                  // Apply all transforms
                  ExtendedRecord verbatim = defaultValuesTransform.convert(er);
                  MultiTaxonRecord tr = taxonomyTransform.convert(er);
                  LocationRecord lr = locationTransform.convert(er, metadata);
                  GrscicollRecord gr = grscicollTransform.convert(er, metadata);
                  TemporalRecord ter = temporalTransform.convert(er);
                  BasicRecord br = basicTransform.convert(er);
                  DnaDerivedDataRecord dr = dnDerivedDataTransform.convert(er);
                  MultimediaRecord mr = multimediaTransform.convert(er);
                  ImageRecord ir = imageTransform.convert(er);
                  AudubonRecord ar = audubonTransform.convert(er);
                  ClusteringRecord cr = clusteringTransform.convert(idr);

                  // merge the multimedia records
                  MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);

                  return Occurrence.builder()
                      .id(simpleRecord.getId())
                      .coreId(simpleRecord.getCoreId())
                      .identifier(simpleRecord.getIdentifier())
                      .verbatim(MAPPER.writeValueAsString(verbatim))
                      .basic(MAPPER.writeValueAsString(br))
                      .taxon(MAPPER.writeValueAsString(tr))
                      .location(MAPPER.writeValueAsString(lr))
                      .grscicoll(MAPPER.writeValueAsString(gr))
                      .temporal(MAPPER.writeValueAsString(ter))
                      .dnaDerivedData(MAPPER.writeValueAsString(dr))
                      .multimedia(MAPPER.writeValueAsString(mmr))
                      .clustering(MAPPER.writeValueAsString(cr))
                      .build();
                },
            Encoders.bean(Occurrence.class));

    try {
      clusteringTransform.close();
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
    }

    // write simple interpreted records to disk
    interpreted.write().mode(SaveMode.Overwrite).parquet(outputPath + "/" + SIMPLE_OCCURRENCE);

    if (useCheckpoints) {
      // re-load
      return spark
          .read()
          .parquet(outputPath + "/" + SIMPLE_OCCURRENCE)
          .as(Encoders.bean(Occurrence.class));
    } else {
      return interpreted;
    }
  }

  /**
   * Loads extended records from the AVRO. Filters out records without core terms and extensions not
   * in the allowed list. Re-partitions the dataset to the specified number of shards. Writes the
   * filtered extended records to Parquet.
   *
   * @param spark The Spark session
   * @param config The pipelines configuration
   * @param inputPath Path to the verbatim AVRO files
   * @param outputPath Path to write the filtered Parquet files
   * @param numberOfShards Number of shards to repartition the dataset
   * @return The filtered and repartitioned dataset of extended records
   */
  public static Dataset<ExtendedRecord> loadExtendedRecords(
      SparkSession spark,
      PipelinesConfig config,
      String inputPath,
      String outputPath,
      int numberOfShards,
      boolean useCheckpoints) {

    final Set<String> allowExtensions =
        Optional.ofNullable(config.getExtensionsAllowedForVerbatimSet())
            .orElse(Collections.emptySet());

    Dataset<ExtendedRecord> extended =
        spark
            .read()
            .format("parquet")
            .load(inputPath + "/" + OCCURRENCE_VERBATIM)
            .as(Encoders.bean(ExtendedRecord.class))
            .filter(
                (FilterFunction<ExtendedRecord>) er -> er != null && !er.getCoreTerms().isEmpty())
            .map(
                (MapFunction<ExtendedRecord, ExtendedRecord>)
                    er -> {
                      Map<String, List<Map<String, String>>> extensions = new HashMap<>();
                      er.getExtensions().entrySet().stream()
                          .filter(es -> allowExtensions.contains(es.getKey()))
                          .filter(es -> !es.getValue().isEmpty())
                          .forEach(es -> extensions.put(es.getKey(), es.getValue()));
                      return ExtendedRecord.newBuilder()
                          .setId(er.getId())
                          .setCoreId(er.getId())
                          .setCoreTerms(er.getCoreTerms())
                          .setExtensions(extensions)
                          .build();
                    },
                Encoders.bean(ExtendedRecord.class))
            .repartition(numberOfShards);

    // write to parquet for downstream steps
    extended.write().mode(SaveMode.Overwrite).parquet(outputPath + "/" + VERBATIM_EXT_FILTERED);
    // reload
    return spark
        .read()
        .parquet(outputPath + "/" + VERBATIM_EXT_FILTERED)
        .as(Encoders.bean(ExtendedRecord.class));
  }

  private static void sparkLog(
      SparkSession spark, String groupId, String message, boolean useCheckpoints) {
    if (useCheckpoints) {
      spark.sparkContext().setJobGroup(groupId, message, true);
    }
  }

  private static Dataset<IdentifierRecord> loadIdentifiers(SparkSession spark, String outputPath) {
    return spark
        .read()
        .parquet(outputPath + "/" + IDENTIFIERS)
        .as(Encoders.bean(IdentifierRecord.class));
  }

  /**
   * Process and then reload identifiers if the /identifiers directory exists, then we assume that
   * the valid and absent identifiers are already present because of a previous interpretation run
   *
   * @param spark
   * @param config
   * @param outputPath
   * @param datasetId
   */
  public static void processIdentifiers(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      String outputPath,
      String datasetId,
      boolean tripletValid,
      boolean occurrenceIdValid)
      throws IOException {

    Path identifiersPath = new Path(outputPath, IDENTIFIERS);
    Path successMarker = new Path(identifiersPath, "_SUCCESS");

    if (fileSystem.exists(identifiersPath) && fileSystem.exists(successMarker)) {
      log.debug("Skipping processing {} - re-using existing identifiers", IDENTIFIERS);
      return;
    }

    // load the valid identifiers - identifiers already present in hbase
    Dataset<IdentifierRecord> identifiers = loadValidIdentifiers(spark, outputPath);

    // persist the absent identifiers - records that need to be assigned an identifier
    // (i.e. added to hbase)
    Dataset<IdentifierRecord> newlyAdded =
        persistAbsentIdentifiers(
            spark, outputPath, config, datasetId, tripletValid, occurrenceIdValid);

    // merge the two datasets
    Dataset<IdentifierRecord> allIdentifiers = identifiers.union(newlyAdded);

    // write out the final identifiers
    allIdentifiers.write().mode(SaveMode.Overwrite).parquet(outputPath + "/" + IDENTIFIERS);
  }

  private static Dataset<IdentifierRecord> persistAbsentIdentifiers(
      SparkSession spark,
      String outputPath,
      PipelinesConfig config,
      String datasetId,
      boolean tripletValid,
      boolean occurrenceIdValid) {

    Dataset<IdentifierRecord> absentIdentifiers =
        spark
            .read()
            .parquet(outputPath + "/" + IDENTIFIERS + "_absent")
            .as(Encoders.bean(IdentifierRecord.class));

    GbifAbsentIdTransform absentIdTransform =
        GbifAbsentIdTransform.builder()
            .isTripletValid(tripletValid)
            .isOccurrenceIdValid(occurrenceIdValid)
            .useExtendedRecordId(false) // validator thing
            .generateIdIfAbsent(true)
            .keygenServiceSupplier(
                (SerializableSupplier<HBaseLockingKey>)
                    () -> KeygenServiceFactory.create(config, datasetId))
            .build();

    // Persist to HBase or any other storage
    Dataset<IdentifierRecord> persisted =
        absentIdentifiers.map(
            (MapFunction<IdentifierRecord, IdentifierRecord>) absentIdTransform::persist,
            Encoders.bean(IdentifierRecord.class));

    try {
      absentIdTransform.close();
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
    }

    return persisted;
  }

  private static Dataset<IdentifierRecord> loadValidIdentifiers(
      SparkSession spark, String outputPath) {
    return spark
        .read()
        .parquet(outputPath + "/" + IDENTIFIERS + "_valid")
        .as(Encoders.bean(IdentifierRecord.class));
  }

  public static Dataset<OccurrenceJsonRecord> toJson(
      Dataset<Occurrence> records, MetadataRecord metadataRecord) {
    return records.map(
        (MapFunction<Occurrence, OccurrenceJsonRecord>)
            record -> {
              OccurrenceJsonConverter c =
                  OccurrenceJsonConverter.builder()
                      .metadata(metadataRecord)
                      .verbatim(MAPPER.readValue(record.getVerbatim(), ExtendedRecord.class))
                      .basic(MAPPER.readValue(record.getBasic(), BasicRecord.class))
                      .location(MAPPER.readValue(record.getLocation(), LocationRecord.class))
                      .temporal(MAPPER.readValue(record.getTemporal(), TemporalRecord.class))
                      .multiTaxon(MAPPER.readValue(record.getTaxon(), MultiTaxonRecord.class))
                      .grscicoll(MAPPER.readValue(record.getGrscicoll(), GrscicollRecord.class))
                      .identifier(MAPPER.readValue(record.getIdentifier(), IdentifierRecord.class))
                      .multimedia(MAPPER.readValue(record.getMultimedia(), MultimediaRecord.class))
                      .dnaDerivedData(
                          MAPPER.readValue(record.getDnaDerivedData(), DnaDerivedDataRecord.class))
                      .clustering(MAPPER.readValue(record.getClustering(), ClusteringRecord.class))
                      .build();

              return c.convert();
            },
        Encoders.bean(OccurrenceJsonRecord.class));
  }

  public static Dataset<OccurrenceHdfsRecord> toHdfs(
      Dataset<Occurrence> records, MetadataRecord metadataRecord) {
    return records.map(
        (MapFunction<Occurrence, OccurrenceHdfsRecord>)
            record -> {
              OccurrenceHdfsRecordConverter c =
                  OccurrenceHdfsRecordConverter.builder()
                      .metadataRecord(metadataRecord)
                      .extendedRecord(MAPPER.readValue(record.getVerbatim(), ExtendedRecord.class))
                      .basicRecord(MAPPER.readValue(record.getBasic(), BasicRecord.class))
                      .locationRecord(MAPPER.readValue(record.getLocation(), LocationRecord.class))
                      .temporalRecord(MAPPER.readValue(record.getTemporal(), TemporalRecord.class))
                      .multiTaxonRecord(MAPPER.readValue(record.getTaxon(), MultiTaxonRecord.class))
                      .grscicollRecord(
                          MAPPER.readValue(record.getGrscicoll(), GrscicollRecord.class))
                      .identifierRecord(
                          MAPPER.readValue(record.getIdentifier(), IdentifierRecord.class))
                      .multimediaRecord(
                          MAPPER.readValue(record.getMultimedia(), MultimediaRecord.class))
                      .dnaDerivedDataRecord(
                          MAPPER.readValue(record.getDnaDerivedData(), DnaDerivedDataRecord.class))
                      .clusteringRecord(
                          MAPPER.readValue(record.getClustering(), ClusteringRecord.class))
                      .build();

              return c.convert();
            },
        Encoders.bean(OccurrenceHdfsRecord.class));
  }
}
