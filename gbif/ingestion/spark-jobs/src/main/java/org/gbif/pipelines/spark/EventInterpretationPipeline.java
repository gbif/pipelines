package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.Directories.*;
import static org.gbif.pipelines.spark.OccurrenceInterpretationPipeline.getMetadataRecord;
import static org.gbif.pipelines.spark.util.LogUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.spark.util.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.*;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.Parent;
import org.gbif.pipelines.io.avro.event.EventHdfsRecord;
import org.gbif.pipelines.io.avro.json.*;
import org.gbif.pipelines.spark.pojo.Event;
import org.gbif.pipelines.spark.pojo.EventLineage;
import org.gbif.pipelines.spark.util.DerivedMetadataUtil;
import org.gbif.pipelines.spark.util.EventInheritanceUtil;
import org.gbif.pipelines.spark.util.LineageUtil;
import org.gbif.pipelines.transform.*;
import scala.Tuple2;

/** Event interpretation pipeline. */
@Slf4j
public class EventInterpretationPipeline {

  static final ObjectMapper MAPPER =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

  public static final String METRICS_FILENAME = "verbatim-to-event.yml";

  private static final List<String> EXTENSIONS_WITH_OCCS =
      List.of(
          Extension.EXTENDED_MEASUREMENT_OR_FACT.getRowType(),
          Extension.DNA_DERIVED_DATA.getRowType());

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = APP_NAME_ARG, description = "Application name", required = true)
    private String appName;

    @Parameter(names = DATASET_ID_ARG, description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = ATTEMPT_ID_ARG, description = "Attempt number", required = true)
    private int attempt;

    @Parameter(names = CONFIG_PATH_ARG, description = "Path to YAML configuration file")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = SPARK_MASTER_ARG,
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(names = NUMBER_OF_SHARDS_ARG, description = "Number of shards", required = false)
    private int numberOfShards = 10;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;

    @Parameter(names = "--useSystemExit", description = "Use checkpoints where possible", arity = 1)
    private boolean useSystemExit = true;
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
    SparkSession spark = null;
    FileSystem fileSystem = null;

    try {
      /* ############ standard init block ########## */
      spark =
          getSparkSession(
              args.master,
              args.appName,
              config,
              OccurrenceInterpretationPipeline::configSparkSession);
      fileSystem = getFileSystem(spark, config);
      /* ############ standard init block - end ########## */

      runEventInterpretation(spark, fileSystem, config, datasetId, attempt, args.numberOfShards);
    } finally {
      if (fileSystem != null) {
        fileSystem.close();
      }
      if (spark != null) {
        spark.stop();
        spark.close();
      }
    }

    if (args.useSystemExit) {
      System.exit(0);
    }
  }

  public static void runEventInterpretation(
      SparkSession spark,
      FileSystem fs,
      PipelinesConfig config,
      String datasetId,
      int attempt,
      int numberOfShards)
      throws Exception {

    long start = System.currentTimeMillis();

    ThreadContext.put("datasetKey", datasetId);
    ThreadContext.put("attempt", String.valueOf(attempt));
    ThreadContext.put("step", StepType.EVENTS_VERBATIM_TO_INTERPRETED.name());
    log.info("Starting event interpretation for attempt {}", attempt);

    String inputPath = String.format("%s/%s/%d", config.getInputPath(), datasetId, attempt);
    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    MetadataRecord metadata = getMetadataRecord(config, datasetId, attempt);

    // Load the extended records
    sparkLog(spark, "event-interpretation", "Loading extended records");
    Dataset<ExtendedRecord> extendedRecords =
        loadExtendedRecords(spark, config, inputPath, outputPath, numberOfShards);

    sparkLog(spark, "event-interpretation", "Generating event lineage");
    Dataset<EventLineage> lineage = generateLineage(spark, extendedRecords);

    // run the record by record transformations
    sparkLog(spark, "event-interpretation", "Running record by record transformations");
    runTransforms(spark, config, extendedRecords, metadata, lineage, outputPath);

    // using the parent lineage, join back to get the full event records
    sparkLog(spark, "event-interpretation", "Event inheritance");
    EventInheritanceUtil.runEventInheritance(spark, outputPath);

    // calculate derived metadata and join to events
    sparkLog(spark, "event-interpretation", "Add derived data");
    DerivedMetadataUtil.addCalculateDerivedMetadata(spark, config, fs, outputPath);

    // load simple records
    Dataset<Event> simpleRecords =
        spark
            .read()
            .parquet(outputPath + "/" + SIMPLE_EVENT_WITH_DERIVED)
            .as(Encoders.bean(Event.class));

    // write parquet for elastic
    sparkLog(spark, "event-interpretation", "Export to JSON parquet");
    toJson(simpleRecords, metadata)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/" + EVENT_JSON);

    // write parquet for hdfs view
    sparkLog(spark, "event-interpretation", "Export to HDFS parquet");
    toHdfs(simpleRecords, metadata)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/" + EVENT_HDFS);

    final long eventCount = extendedRecords.count();

    // write metrics to yaml
    writeMetricsYaml(
        fs,
        Map.of(PipelinesVariables.Metrics.BASIC_RECORDS_COUNT, eventCount),
        outputPath + "/" + METRICS_FILENAME);

    log.info(timeAndRecPerSecond("events-interpretation", start, eventCount));
  }

  private static Dataset<EventLineage> generateLineage(
      SparkSession spark, Dataset<ExtendedRecord> extendedRecords) {

    StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("eventId", DataTypes.StringType, true),
              DataTypes.createStructField("eventType", DataTypes.StringType, true),
              DataTypes.createStructField("parentEventId", DataTypes.StringType, true)
            });

    Dataset<Row> events =
        extendedRecords.map(
            (MapFunction<ExtendedRecord, Row>)
                record -> {
                  String eventID = ModelUtils.extractValue(record, DwcTerm.eventID);
                  Optional<String> eventTypeOpt =
                      ModelUtils.extractOptValue(record, DwcTerm.eventType);
                  Optional<String> parentEventIDOpt =
                      ModelUtils.extractOptValue(record, DwcTerm.parentEventID);
                  return RowFactory.create(
                      eventID, eventTypeOpt.orElse(null), parentEventIDOpt.orElse(null));
                },
            Encoders.row(schema));

    return LineageUtil.calculateLineage(spark, events);
  }

  private static Dataset<ParentJsonRecord> toJson(
      Dataset<Event> simpleRecords, MetadataRecord metadata) {
    return simpleRecords.map(
        (MapFunction<Event, ParentJsonRecord>)
            r -> {
              String eventCoreInherited = r.getEventInherited();
              String locationInherited = r.getLocationInherited();
              String temporalInherited = r.getTemporalInherited();
              EventInheritedRecord eventInheritedRecord = null;
              LocationInheritedRecord locationInheritedRecord = null;
              TemporalInheritedRecord temporalInheritedRecord = null;
              DerivedMetadataRecord derivedMetadataRecord = null;

              if (eventCoreInherited != null) {
                eventInheritedRecord =
                    MAPPER.readValue(
                        r.getEventInherited(),
                        org.gbif.pipelines.io.avro.json.EventInheritedRecord.class);
              }
              if (locationInherited != null) {
                locationInheritedRecord =
                    MAPPER.readValue(
                        r.getLocationInherited(),
                        org.gbif.pipelines.io.avro.json.LocationInheritedRecord.class);
              }
              if (temporalInherited != null) {
                temporalInheritedRecord =
                    MAPPER.readValue(
                        r.getTemporalInherited(),
                        org.gbif.pipelines.io.avro.json.TemporalInheritedRecord.class);
              }

              if (r.getDerivedMetadata() != null && !r.getDerivedMetadata().isEmpty()) {
                MAPPER.readValue(
                    r.getDerivedMetadata(),
                    org.gbif.pipelines.io.avro.json.DerivedMetadataRecord.class);
              } else {
                derivedMetadataRecord = new DerivedMetadataRecord();
                derivedMetadataRecord.setId(r.getId());
              }

              ParentJsonConverter c =
                  ParentJsonConverter.builder()
                      .metadata(metadata)
                      .eventCore(MAPPER.readValue(r.getEventCore(), EventCoreRecord.class))
                      .identifier(MAPPER.readValue(r.getIdentifier(), IdentifierRecord.class))
                      .verbatim(MAPPER.readValue(r.getVerbatim(), ExtendedRecord.class))
                      .temporal(MAPPER.readValue(r.getTemporal(), TemporalRecord.class))
                      .location(MAPPER.readValue(r.getLocation(), LocationRecord.class))
                      .humboldtRecord(MAPPER.readValue(r.getHumboldt(), HumboldtRecord.class))
                      .multimedia(MAPPER.readValue(r.getMultimedia(), MultimediaRecord.class))
                      .eventInheritedRecord(eventInheritedRecord)
                      .locationInheritedRecord(locationInheritedRecord)
                      .temporalInheritedRecord(temporalInheritedRecord)
                      .derivedMetadata(derivedMetadataRecord)
                      .build();
              return c.convertToParent();
            },
        Encoders.bean(ParentJsonRecord.class));
  }

  private static Dataset<EventHdfsRecord> toHdfs(
      Dataset<Event> simpleRecords, MetadataRecord metadata) {
    return simpleRecords.map(
        (MapFunction<Event, EventHdfsRecord>)
            record -> {
              EventHdfsRecordConverter c =
                  EventHdfsRecordConverter.builder()
                      .metadataRecord(metadata)
                      .extendedRecord(MAPPER.readValue(record.getVerbatim(), ExtendedRecord.class))
                      .locationRecord(MAPPER.readValue(record.getLocation(), LocationRecord.class))
                      .temporalRecord(MAPPER.readValue(record.getTemporal(), TemporalRecord.class))
                      .identifierRecord(
                          MAPPER.readValue(record.getIdentifier(), IdentifierRecord.class))
                      .multimediaRecord(
                          MAPPER.readValue(record.getMultimedia(), MultimediaRecord.class))
                      .eventCoreRecord(
                          MAPPER.readValue(record.getEventCore(), EventCoreRecord.class))
                      .humboldtRecord(MAPPER.readValue(record.getHumboldt(), HumboldtRecord.class))
                      .build();

              return c.convert();
            },
        Encoders.bean(EventHdfsRecord.class));
  }

  public static void sparkLog(SparkSession spark, String groupId, String message) {
    spark.sparkContext().setJobGroup(groupId, message, true);
  }

  public static Dataset<Event> runTransforms(
      SparkSession spark,
      PipelinesConfig config,
      Dataset<ExtendedRecord> extendedRecords,
      MetadataRecord metadata,
      Dataset<EventLineage> lineage,
      String outputPath) {

    // Used transforms
    LocationTransform locationTransform = LocationTransform.create(config);
    TemporalTransform temporalTransform = TemporalTransform.create(config);
    MultimediaTransform multimediaTransform = MultimediaTransform.create(config);
    AudubonTransform audubonTransform = AudubonTransform.create(config);
    ImageTransform imageTransform = ImageTransform.create(config);
    EventCoreTransform eventCoreTransform = EventCoreTransform.create(config);
    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.create(config);
    HumboldtTransform humboldtTransform = HumboldtTransform.create(config);
    IdentifierTransform identifierTransform = IdentifierTransform.create();

    // join with lineage
    Dataset<Tuple2<ExtendedRecord, EventLineage>> join =
        extendedRecords
            .as("extendedRecord")
            .joinWith(
                lineage,
                extendedRecords
                    .col("coreTerms.`http://rs.tdwg.org/dwc/terms/eventID`")
                    .equalTo(lineage.col("id")),
                "left_outer");

    Dataset<Event> interpreted =
        join.map(
            (MapFunction<Tuple2<ExtendedRecord, EventLineage>, Event>)
                row -> {
                  ExtendedRecord verbatim = row._1;
                  EventLineage eventLineage = row._2;
                  IdentifierRecord idr =
                      identifierTransform.convert(verbatim, metadata.getDatasetKey());
                  LocationRecord lr = locationTransform.convert(verbatim, metadata);
                  TemporalRecord ter = temporalTransform.convert(verbatim);
                  MultimediaRecord mr = multimediaTransform.convert(verbatim);
                  ImageRecord ir = imageTransform.convert(verbatim);
                  AudubonRecord ar = audubonTransform.convert(verbatim);
                  MeasurementOrFactRecord mfr = measurementOrFactTransform.convert(verbatim);
                  EventCoreRecord ecr = eventCoreTransform.convert(verbatim, null);
                  HumboldtRecord hr = humboldtTransform.convert(verbatim);

                  // add the lineage
                  if (eventLineage != null) {
                    ecr.setParentsLineage(eventLineage.getLineage());
                  }

                  // merge the multimedia records
                  MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);

                  // get lineage ids
                  List<String> lineageIds =
                      Optional.ofNullable(eventLineage)
                          .map(EventLineage::getLineage)
                          .orElseGet(List::of)
                          .stream()
                          .map(Parent::getId)
                          .toList();

                  return Event.builder()
                      .id(verbatim.getId())
                      .lineage(lineageIds)
                      .identifier(MAPPER.writeValueAsString(idr))
                      .verbatim(MAPPER.writeValueAsString(verbatim))
                      .location(MAPPER.writeValueAsString(lr))
                      .temporal(MAPPER.writeValueAsString(ter))
                      .multimedia(MAPPER.writeValueAsString(mmr))
                      .measurementOrFact(MAPPER.writeValueAsString(mfr))
                      .eventCore(MAPPER.writeValueAsString(ecr))
                      .humboldt(MAPPER.writeValueAsString(hr))
                      .build();
                },
            Encoders.bean(Event.class));

    // write simple interpreted records to disk
    interpreted.write().mode(SaveMode.Overwrite).parquet(outputPath + "/" + SIMPLE_EVENT);

    // re-load
    return spark.read().parquet(outputPath + "/" + SIMPLE_EVENT).as(Encoders.bean(Event.class));
  }

  private static Dataset<ExtendedRecord> loadExtendedRecords(
      SparkSession spark,
      PipelinesConfig config,
      String inputPath,
      String outputPath,
      int numberOfShards) {

    final Set<String> allowExtensions =
        Optional.ofNullable(config.getExtensionsAllowedForVerbatimSet())
            .orElse(Collections.emptySet());

    Dataset<ExtendedRecord> extended =
        spark
            .read()
            .format("avro")
            .load(inputPath + "/verbatim.avro")
            .repartition(numberOfShards)
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
                          .forEach(
                              es -> {
                                if (EXTENSIONS_WITH_OCCS.contains(es.getKey())) {
                                  List<Map<String, String>> parsedExtension =
                                      es.getValue().stream()
                                          .filter(
                                              v ->
                                                  !v.containsKey(
                                                      DwcTerm.occurrenceID.qualifiedName()))
                                          .collect(Collectors.toList());

                                  if (!parsedExtension.isEmpty()) {
                                    extensions.put(es.getKey(), parsedExtension);
                                  }
                                } else {
                                  extensions.put(es.getKey(), es.getValue());
                                }
                              });
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
    extended
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/" + VERBATIM_EVENT_EXT_FILTERED);
    // reload
    return spark
        .read()
        .parquet(outputPath + "/" + VERBATIM_EVENT_EXT_FILTERED)
        .as(Encoders.bean(ExtendedRecord.class));
  }
}
