package org.gbif.pipelines.spark;

import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.coordinator.DistributedUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.spark.Directories.*;
import static org.gbif.pipelines.spark.OccurrenceInterpretation.getMetadataRecord;
import static org.gbif.pipelines.spark.OccurrenceInterpretation.loadExtendedRecords;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.*;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;
import org.gbif.pipelines.transform.*;
import org.slf4j.MDC;
import scala.Tuple2;

@Slf4j
public class EventInterpretation {

  static final ObjectMapper MAPPER = new ObjectMapper();

  public static final String METRICS_FILENAME = "verbatim-to-event.yml";

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

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

    @Parameter(names = "--numberOfShards", description = "Number of shards", required = false)
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

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master, args.appName, config, OccurrenceInterpretation::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);
    /* ############ standard init block - end ########## */

    runEventInterpretation(spark, fileSystem, config, datasetId, attempt, args.numberOfShards);

    fileSystem.close();
    spark.stop();
    spark.close();
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

    MDC.put("datasetKey", datasetId);
    log.info("Starting event interpretation");

    String inputPath = String.format("%s/%s/%d", config.getInputPath(), datasetId, attempt);
    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    MetadataRecord metadata = getMetadataRecord(config, datasetId);

    // Load the extended records
    Dataset<ExtendedRecord> extendedRecords =
        loadExtendedRecords(spark, config, inputPath, outputPath, numberOfShards);

    Dataset<EventLineage> lineage = generateLineage(spark, extendedRecords);

    if (log.isDebugEnabled()) {
      lineage.show(10, false);
    }

    // run the record by record transformations
    runTransforms(spark, config, extendedRecords, metadata, lineage, outputPath);

    // using the parent lineage, join back to get the full event records
    EventInheritance.runEventInheritance(spark, outputPath);

    // calculate derived metadata and join to events
    Dataset<Event> simpleRecords =
        CalculateDerivedMetadata.addCalculateDerivedMetadata(spark, fs, outputPath);

    // write parquet for elastic
    toJson(simpleRecords, metadata)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/" + EVENT_JSON);

    // write parquet for hdfs view
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
                  String eventID = record.getId();
                  Optional<String> eventTypeOpt =
                      ModelUtils.extractOptValue(record, DwcTerm.eventType);
                  Optional<String> parentEventIDOpt =
                      ModelUtils.extractOptValue(record, DwcTerm.parentEventID);
                  return RowFactory.create(
                      eventID, eventTypeOpt.orElse(null), parentEventIDOpt.orElse(null));
                },
            Encoders.row(schema));

    return CalculateLineage.calculateLineage(spark, events);
  }

  private static Dataset<ParentJsonRecord> toJson(
      Dataset<Event> simpleRecords, MetadataRecord metadata) {
    return simpleRecords.map(
        (MapFunction<Event, ParentJsonRecord>)
            r -> {
              ParentJsonConverter c =
                  ParentJsonConverter.builder()
                      .metadata(metadata)
                      .eventCore(MAPPER.readValue(r.getEventCore(), EventCoreRecord.class))
                      .identifier(MAPPER.readValue(r.getIdentifier(), IdentifierRecord.class))
                      .verbatim(MAPPER.readValue(r.getVerbatim(), ExtendedRecord.class))
                      .temporal(MAPPER.readValue(r.getTemporal(), TemporalRecord.class))
                      .location(MAPPER.readValue(r.getLocation(), LocationRecord.class))
                      .measurementOrFactRecord(
                          MAPPER.readValue(r.getMeasurementOrFact(), MeasurementOrFactRecord.class))
                      .humboldtRecord(MAPPER.readValue(r.getHumboldt(), HumboldtRecord.class))
                      .multimedia(MAPPER.readValue(r.getMultimedia(), MultimediaRecord.class))
                      .eventInheritedRecord(
                          MAPPER.readValue(
                              r.getEventInherited(),
                              org.gbif.pipelines.io.avro.json.EventInheritedRecord.class))
                      .locationInheritedRecord(
                          MAPPER.readValue(
                              r.getLocationInherited(),
                              org.gbif.pipelines.io.avro.json.LocationInheritedRecord.class))
                      .temporalInheritedRecord(
                          MAPPER.readValue(
                              r.getTemporalInherited(),
                              org.gbif.pipelines.io.avro.json.TemporalInheritedRecord.class))
                      .derivedMetadata(
                          MAPPER.readValue(
                              r.getDerivedMetadata(),
                              org.gbif.pipelines.io.avro.json.DerivedMetadataRecord.class))
                      .build();
              return c.convertToParent();
            },
        Encoders.bean(ParentJsonRecord.class));
  }

  private static Dataset<OccurrenceHdfsRecord> toHdfs(
      Dataset<Event> simpleRecords, MetadataRecord metadata) {
    return simpleRecords.map(
        (MapFunction<Event, OccurrenceHdfsRecord>)
            record -> {
              OccurrenceHdfsRecordConverter c =
                  OccurrenceHdfsRecordConverter.builder()
                      .metadataRecord(metadata)
                      .extendedRecord(MAPPER.readValue(record.getVerbatim(), ExtendedRecord.class))
                      .locationRecord(MAPPER.readValue(record.getLocation(), LocationRecord.class))
                      .temporalRecord(MAPPER.readValue(record.getTemporal(), TemporalRecord.class))
                      .multiTaxonRecord(MAPPER.readValue(record.getTaxon(), MultiTaxonRecord.class))
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
        Encoders.bean(OccurrenceHdfsRecord.class));
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
    MultiTaxonomyTransform taxonomyTransform = MultiTaxonomyTransform.create(config);
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
            .joinWith(lineage, extendedRecords.col("id").equalTo(lineage.col("id")), "left_outer");

    Dataset<Event> interpreted =
        join.map(
            (MapFunction<Tuple2<ExtendedRecord, EventLineage>, Event>)
                row -> {
                  ExtendedRecord verbatim = row._1;
                  EventLineage eventLineage = row._2;
                  IdentifierRecord idr =
                      identifierTransform.convert(verbatim, metadata.getDatasetKey());
                  MultiTaxonRecord tr = taxonomyTransform.convert(verbatim);
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
                      .taxon(MAPPER.writeValueAsString(tr))
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
}
