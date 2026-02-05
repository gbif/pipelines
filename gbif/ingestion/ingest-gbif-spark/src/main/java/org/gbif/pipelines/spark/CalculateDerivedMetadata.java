package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.col;
import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.Directories.*;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.SparkUtil.getSparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.JsonConverter;
import org.gbif.pipelines.core.parsers.location.parser.ConvexHullParser;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.DerivedClassification;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.io.avro.json.TaxonCoverage;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import scala.Tuple2;
import scala.Tuple3;

@Slf4j
public class CalculateDerivedMetadata implements Serializable {

  static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {

    PipelinesConfig config =
        loadConfig(
            "/Users/djtfmartin/dev/my-forks/pipelines/gbif/ingestion/ingest-gbif-spark/pipelines.yaml");
    String datasetId = "ecebee66-f913-4105-acb6-738430d0edc9";
    int attempt = 1;

    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    SparkSession spark =
        getSparkSession("local[*]", "My app", config, OccurrenceInterpretation::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    runCalculateDerivedMetadata(spark, fileSystem, outputPath);

    fileSystem.close();
    spark.stop();
    spark.close();
    System.exit(0);
  }

  public static Dataset<Event> addCalculateDerivedMetadata(
      SparkSession spark, FileSystem fs, String outputPath) throws IOException {

    Dataset<DerivedMetadataRecord> derivedRecords =
        runCalculateDerivedMetadata(spark, fs, outputPath);

    // load simple event
    Dataset<Event> events =
        spark.read().parquet(outputPath + "/" + SIMPLE_EVENT).as(Encoders.bean(Event.class));

    // join events and derived metadata
    events
        .joinWith(derivedRecords, events.col("id").equalTo(derivedRecords.col("id")), "left_outer")
        .map(
            (MapFunction<Tuple2<Event, DerivedMetadataRecord>, Event>)
                row -> {
                  Event event = row._1();
                  DerivedMetadataRecord derived = row._2();
                  if (derived != null) {
                    event.setDerivedMetadata(MAPPER.writeValueAsString(derived));
                  }
                  return event;
                },
            Encoders.bean(Event.class))
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/" + SIMPLE_EVENT_WITH_DERIVED);

    return spark
        .read()
        .parquet(outputPath + "/" + SIMPLE_EVENT_WITH_DERIVED)
        .as(Encoders.bean(Event.class));
  }

  /**
   * Main method that calculates derived metadata for events based on occurrences and child events.
   *
   * <p>This includes calculating spatial convex hulls and temporal coverage.
   *
   * @param spark Spark session
   * @param fileSystem Hadoop file system
   * @param outputPath Path to the dataset output
   * @return Dataset of DerivedMetadataRecord
   * @throws IOException if there is an error reading or writing data
   */
  public static Dataset<DerivedMetadataRecord> runCalculateDerivedMetadata(
      SparkSession spark, FileSystem fileSystem, String outputPath) throws IOException {

    // loads events
    Dataset<Event> events =
        spark.read().parquet(outputPath + "/" + SIMPLE_EVENT).as(Encoders.bean(Event.class));

    // load occurrences (handling datasets without occurrences)
    Dataset<Occurrence> occurrence = loadOccurrences(spark, fileSystem, outputPath);
    log.info("occurrences {}", occurrence.count());

    // Calculate Convex Hull
    Dataset<Tuple2<String, String>> eventIdConvexHull =
        calculateConvexHull(spark, outputPath, events, occurrence);

    // Calculate Temporal Coverage
    Dataset<Tuple3<String, String, String>> temporalCoverages =
        calculateTemporalCoverage(spark, outputPath, events, occurrence);

    // Calculate Taxonomic Coverage
    Dataset<Tuple2<String, String>> taxonomicCoverages =
        calculateTaxonomicCoverage(outputPath, occurrence);

    return createDerivedDataRecords(
        spark, outputPath, events, eventIdConvexHull, temporalCoverages, taxonomicCoverages);
  }

  public static Dataset<Tuple2<String, String>> calculateTaxonomicCoverage(
      String outputPath, Dataset<Occurrence> occurrence) {

    ObjectMapper MAPPER = new ObjectMapper();

    // creates a EventID -> TaxonCoverage map
    Dataset<EventTaxonCoverage> taxonomicCoverages =
        occurrence
            .map(
                (MapFunction<Occurrence, EventTaxonCoverage>)
                    occ -> {
                      String eventId = occ.getCoreId();
                      Set<String> taxonIDs = new HashSet<>();
                      Map<String, List<DerivedClassification>> classifications = new HashMap<>();

                      // retrieve taxonID from verbatim
                      ExtendedRecord verbatim =
                          MAPPER.readValue(occ.getVerbatim(), ExtendedRecord.class);
                      ModelUtils.extractOptValue(verbatim, DwcTerm.taxonID)
                          .ifPresent(taxonIDs::add);

                      // map taxon records to classifications
                      String taxonJson = occ.getTaxon();
                      if (taxonJson != null) {

                        MultiTaxonRecord multiTaxonRecord =
                            MAPPER.readValue(taxonJson, MultiTaxonRecord.class);

                        if (multiTaxonRecord.getTaxonRecords() != null) {

                          multiTaxonRecord
                              .getTaxonRecords()
                              .forEach(
                                  tr -> {
                                    DerivedClassification classification =
                                        JsonConverter.convertTaxonRecordToDerivedClassification(tr);
                                    classifications.put(
                                        tr.getDatasetKey(), List.of(classification));
                                  });
                        }
                      }

                      TaxonCoverage taxonCoverage =
                          TaxonCoverage.newBuilder()
                              .setTaxonIDs(taxonIDs.stream().toList())
                              .setClassifications(classifications)
                              .build();

                      return new EventTaxonCoverage(
                          eventId, MAPPER.writeValueAsString(taxonCoverage));
                    },
                Encoders.bean(EventTaxonCoverage.class))
            .distinct();

    // Merge partial - from the previous stage we have multiple taxonomic coverages per event
    // EVENT1 -> TC1
    // EVENT1 -> TC2
    // EVENT2 -> TC3
    // Consolidate to EVENT1 -> TC1 + TC2
    Dataset<EventTaxonCoverage> partialMergedCoverages =
        taxonomicCoverages.mapPartitions(
            (MapPartitionsFunction<EventTaxonCoverage, EventTaxonCoverage>)
                iter -> {

                  // Eventid -> list of taxonomic coverages
                  Map<String, TaxonCoverage> acc = new HashMap<>();

                  while (iter.hasNext()) {

                    EventTaxonCoverage tc = iter.next();

                    // get existed taxon coverage in the map for eventID
                    TaxonCoverage existedForEventID = acc.get(tc.eventId);

                    if (existedForEventID == null) {
                      TaxonCoverage newTaxonCoverage =
                          MAPPER.readValue(tc.getTaxonCoverage(), TaxonCoverage.class);
                      acc.put(tc.getEventId(), newTaxonCoverage);
                    } else {
                      TaxonCoverage newTaxonCoverage =
                          MAPPER.readValue(tc.getTaxonCoverage(), TaxonCoverage.class);

                      // merge taxonIDs
                      Set<String> mergedTaxonIDs = new HashSet<>(existedForEventID.getTaxonIDs());
                      mergedTaxonIDs.addAll(newTaxonCoverage.getTaxonIDs());
                      existedForEventID.setTaxonIDs(new ArrayList<>(mergedTaxonIDs));

                      // merge classifications
                      Map<String, List<DerivedClassification>> mergedClassifications =
                          existedForEventID.getClassifications();
                      newTaxonCoverage
                          .getClassifications()
                          .forEach(
                              (key, value) -> {
                                mergedClassifications.merge(
                                    key,
                                    value,
                                    (oldList, newList) -> {
                                      Set<DerivedClassification> set = new HashSet<>(oldList);
                                      set.addAll(newList);
                                      return new ArrayList<>(set);
                                    });
                              });
                      existedForEventID.setClassifications(mergedClassifications);
                    }
                  }

                  return acc.entrySet().stream()
                      .map(
                          e -> {
                            try {
                              return new EventTaxonCoverage(
                                  e.getKey(), // eventID
                                  MAPPER.writeValueAsString(
                                      e.getValue()) // merged TaxonCoverage as JSON
                                  );
                            } catch (Exception ex) {
                              throw new RuntimeException(ex);
                            }
                          })
                      .iterator();
                },
            Encoders.bean(EventTaxonCoverage.class));

    // group by eventID to prepare for final merge
    KeyValueGroupedDataset<String, EventTaxonCoverage> groupedPartial =
        partialMergedCoverages.groupByKey(
            (MapFunction<EventTaxonCoverage, String>) EventTaxonCoverage::getEventId,
            Encoders.STRING());

    Dataset<Tuple2<String, String>> grouped =
        groupedPartial
            .reduceGroups(
                (ReduceFunction<EventTaxonCoverage>)
                    (a, b) -> {
                      EventTaxonCoverage merged = new EventTaxonCoverage(a.getEventId(), null);
                      TaxonCoverage taxonCoverageA =
                          MAPPER.readValue(a.getTaxonCoverage(), TaxonCoverage.class);
                      TaxonCoverage taxonCoverageB =
                          MAPPER.readValue(b.getTaxonCoverage(), TaxonCoverage.class);

                      taxonCoverageA.getTaxonIDs().addAll(taxonCoverageB.getTaxonIDs());

                      // checklistKey -> DerivedClassification
                      Map<String, List<DerivedClassification>> classificationsA =
                          taxonCoverageA.getClassifications();
                      Map<String, List<DerivedClassification>> classificationsB =
                          taxonCoverageB.getClassifications();

                      Map<String, List<DerivedClassification>> mergedClassifications =
                          new HashMap<>(classificationsA);

                      classificationsA.forEach(
                          (checklistKey, classListA) -> {
                            if (classificationsB.containsKey(checklistKey)) {
                              List<DerivedClassification> classListB =
                                  classificationsB.get(checklistKey);
                              Set<DerivedClassification> mergedSet = new HashSet<>(classListA);
                              mergedSet.addAll(classListB);
                              mergedClassifications.put(checklistKey, new ArrayList<>(mergedSet));
                            }
                          });
                      taxonCoverageA.setClassifications(mergedClassifications);

                      return new EventTaxonCoverage(
                          a.getEventId(), MAPPER.writeValueAsString(taxonCoverageA));
                    })
            .map(
                (MapFunction<Tuple2<String, EventTaxonCoverage>, Tuple2<String, String>>)
                    row -> {
                      return new Tuple2<>(row._1, row._2.getTaxonCoverage());
                    },
                Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

    grouped.write().mode(SaveMode.Overwrite).parquet(outputPath + "/derived/taxon_coverage");

    return grouped;
  }

  /**
   * Creates derived metadata records by joining convex hulls and temporal coverages.
   *
   * @param outputPath path to save the derived metadata
   * @param eventIdConvexHull
   * @param temporalCoverages
   * @return
   */
  @NotNull
  public static Dataset<DerivedMetadataRecord> createDerivedDataRecords(
      SparkSession spark,
      String outputPath,
      Dataset<Event> events,
      Dataset<Tuple2<String, String>> eventIdConvexHull,
      Dataset<Tuple3<String, String, String>> temporalCoverages,
      Dataset<Tuple2<String, String>> taxonomicCoverages) {

    events.select(col("id").as("eventId")).toDF("eventId").createOrReplaceTempView("events");
    eventIdConvexHull.toDF("eventId", "convexHull").createOrReplaceTempView("hulls");
    temporalCoverages.toDF("eventId", "lte", "gte").createOrReplaceTempView("temporal");
    taxonomicCoverages.toDF("eventId", "taxonCoverage").createOrReplaceTempView("taxonomic");

    Dataset<Row> df1 =
        spark
            .sql(
                """
                SELECT
                       e.eventId as eventId,
                       h.convexHull as convexHull,
                       te.lte as lte,
                       te.gte as gte,
                       ta.taxonCoverage as taxonCoverage
                FROM events e
                LEFT OUTER JOIN hulls h
                ON e.eventId = h.eventId
                LEFT OUTER JOIN temporal te
                ON h.eventId = te.eventId
                LEFT OUTER JOIN taxonomic ta
                ON h.eventId = ta.eventId
                """)
            .cache();

    Dataset<DerivedMetadataRecord> derivedMetadataRecordDataset =
        df1.map(
            (MapFunction<Row, DerivedMetadataRecord>)
                row -> {
                  String eventId = row.getAs("eventId");
                  String convexHull = row.getAs("convexHull");
                  String lte = row.getAs("lte");
                  String gte = row.getAs("gte");
                  String taxonCoverageJson = row.getAs("taxonCoverage");

                  TaxonCoverage taxonCoverage = null;

                  if (taxonCoverageJson != null) {
                    taxonCoverage = MAPPER.readValue(taxonCoverageJson, TaxonCoverage.class);
                  }

                  // create derived metadata record
                  DerivedMetadataRecord.Builder builder =
                      DerivedMetadataRecord.newBuilder().setId(eventId);
                  builder.setWktConvexHull(convexHull);
                  builder.setTemporalCoverage(
                      org.gbif.pipelines.io.avro.json.EventDate.newBuilder()
                          .setGte(gte)
                          .setLte(lte)
                          .build());
                  builder.setTaxonomicCoverage(taxonCoverage);
                  return builder.build();
                },
            Encoders.bean(DerivedMetadataRecord.class));

    derivedMetadataRecordDataset
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/event_derived_metadata");

    return derivedMetadataRecordDataset;
  }

  @NotNull
  public static Dataset<Tuple3<String, String, String>> calculateTemporalCoverage(
      SparkSession spark,
      String outputPath,
      Dataset<Event> events,
      Dataset<Occurrence> occurrence) {
    Dataset<Tuple2<String, EventDate>> eventIdToEventDate =
        gatherEventDatesFromChildEvents(spark, events);
    log.info("eventIdToEventDate {}", eventIdToEventDate.count());

    // get unique occurrence temporal - coreId -> eventDate
    Dataset<Tuple2<String, EventDate>> coredIdOccurrenceEventDates =
        getCoreIdEventDates(occurrence);
    log.info("coredIdOccurrenceEventDates {}", coredIdOccurrenceEventDates.count());

    KeyValueGroupedDataset<String, Tuple2<String, EventDate>> groupedByIdDates =
        coredIdOccurrenceEventDates
            .union(eventIdToEventDate)
            .distinct()
            .groupByKey(
                (MapFunction<Tuple2<String, EventDate>, String>) Tuple2::_1, Encoders.STRING());

    Dataset<Tuple3<String, String, String>> temporalCoverages =
        groupedByIdDates.mapGroups(
            (MapGroupsFunction<String, Tuple2<String, EventDate>, Tuple3<String, String, String>>)
                (eventId, eventIter) -> {
                  TemporalAccum accum = new TemporalAccum();
                  eventIter.forEachRemaining(
                      eventDate -> {
                        accum.setMinDate(eventDate._2().getGte());
                        accum.setMaxDate(eventDate._2().getLte());
                      });
                  return new Tuple3<String, String, String>(
                      eventId,
                      accum.toEventDate().get().getLte(),
                      accum.toEventDate().get().getGte());
                },
            Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()));

    temporalCoverages
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/derived/temporal_coverage");
    return temporalCoverages;
  }

  @NotNull
  private static Dataset<Tuple2<String, String>> calculateConvexHull(
      SparkSession spark,
      String outputPath,
      Dataset<Event> events,
      Dataset<Occurrence> occurrence) {
    // join to child events to get all coordinates associated with parent event
    Dataset<EventCoordinate> eventIdToCoordinates = gatherCoordinatesFromChildEvents(spark, events);
    log.info("eventIdToCoordinates {}", eventIdToCoordinates.count());

    // join child events ?
    Dataset<EventCoordinate> coreIdEventCoordinates = getEventCoordinates(events);
    log.info("coreIdEventCoordinates {}", coreIdEventCoordinates.count());

    // get unique occurrence locations - coredId -> "lat||long"
    Dataset<EventCoordinate> coreIdOccurrenceCoordinates = getCoreIdCoordinates(occurrence);
    log.info("coreIdOccurrenceCoordinates {}", coreIdOccurrenceCoordinates.count());

    // Calculate Convex Hull
    Dataset<EventCoordinate> eventIdEventCoordinates =
        coreIdOccurrenceCoordinates
            .union(eventIdToCoordinates)
            .union(coreIdEventCoordinates)
            .distinct();

    // Warning - this has the potential to OOM if there are too many coordinates for an event
    Dataset<Tuple2<String, String>> partialHulls =
        eventIdEventCoordinates.mapPartitions(
            (MapPartitionsFunction<EventCoordinate, Tuple2<String, String>>)
                iter -> {

                  // eventid -> list of coordinates
                  Map<String, List<Coordinate>> acc = new HashMap<>();

                  while (iter.hasNext()) {
                    EventCoordinate ec = iter.next();
                    acc.computeIfAbsent(ec.getEventId(), k -> new ArrayList<>())
                        .add(new Coordinate(ec.getLongitude(), ec.getLatitude()));
                  }

                  List<Tuple2<String, String>> out = new ArrayList<>();
                  for (Map.Entry<String, List<Coordinate>> e : acc.entrySet()) {
                    Geometry hull = ConvexHullParser.fromCoordinates(e.getValue()).getConvexHull();

                    String partialHull = new WKTWriter().write(hull);
                    out.add(new Tuple2<>(e.getKey(), partialHull));
                  }

                  return out.iterator();
                },
            Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

    KeyValueGroupedDataset<String, Tuple2<String, String>> groupedPartial =
        partialHulls.groupByKey(
            (MapFunction<Tuple2<String, String>, String>) Tuple2::_1, Encoders.STRING());

    // merge partial hulls
    Dataset<Tuple2<String, String>> hulls =
        groupedPartial
            .reduceGroups(
                (ReduceFunction<Tuple2<String, String>>)
                    (a, b) -> {
                      List<Coordinate> mergedCoords = new ArrayList<>();
                      String eventId = a._1();
                      String wkt1 = a._2();
                      String wkt2 = b._2();

                      GeometryFactory geometryFactory = new GeometryFactory();
                      WKTReader reader = new WKTReader(geometryFactory);

                      Geometry geometry1 = reader.read(wkt1);
                      Geometry geometry2 = reader.read(wkt2);
                      mergedCoords.addAll(Arrays.asList(geometry1.getCoordinates()));
                      mergedCoords.addAll(Arrays.asList(geometry2.getCoordinates()));
                      Geometry mergedGeom =
                          ConvexHullParser.fromCoordinates(mergedCoords).getConvexHull();
                      String mergedWkt = new WKTWriter().write(mergedGeom);
                      return new Tuple2(eventId, mergedWkt);
                    })
            .map(
                (MapFunction<Tuple2<String, Tuple2<String, String>>, Tuple2<String, String>>)
                    t -> {
                      return new Tuple2<>(t._1, t._2._2);
                    },
                Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

    hulls.write().mode(SaveMode.Overwrite).parquet(outputPath + "/derived/convex_hull");

    return hulls;
  }

  public static Dataset<Tuple2<String, EventDate>> gatherEventDatesFromChildEvents(
      SparkSession spark, Dataset<Event> events) {
    events.createOrReplaceTempView("simple_event");
    return spark
        .sql(
            """
                    SELECT
                           parent_event.id as eventId,
                           child_event.temporal
                    FROM simple_event parent_event
                    LEFT OUTER JOIN simple_event child_event
                    ON array_contains(child_event.lineage, parent_event.id)
                """)
        .filter(
            (FilterFunction<Row>)
                row -> {
                  String temporalJson = row.getAs("temporal");
                  if (temporalJson == null) {
                    return false;
                  }
                  TemporalRecord temporalRecord =
                      MAPPER.readValue(temporalJson, TemporalRecord.class);
                  return temporalRecord != null && temporalRecord.getEventDate() != null;
                })
        .map(
            (MapFunction<Row, Tuple2<String, EventDate>>)
                row -> {
                  String eventId = row.getAs("eventId");
                  String temporalJson = row.getAs("temporal");
                  TemporalRecord temporalRecord =
                      MAPPER.readValue(temporalJson, TemporalRecord.class);
                  return new Tuple2(eventId, temporalRecord.getEventDate());
                },
            Encoders.tuple(Encoders.STRING(), Encoders.bean(EventDate.class)));
  }

  public static Dataset<EventCoordinate> gatherCoordinatesFromChildEvents(
      SparkSession spark, Dataset<Event> events) {
    events.createOrReplaceTempView("simple_event");
    return spark
        .sql(
            """
                    SELECT
                           parent_event.id as eventId,
                           child_event.location
                    FROM simple_event parent_event
                    LEFT OUTER JOIN simple_event child_event
                    ON array_contains(child_event.lineage, parent_event.id)
                """)
        .filter(
            (FilterFunction<Row>)
                row -> {
                  String locationJson = row.getAs("location");
                  if (locationJson == null) {
                    return false;
                  }
                  LocationRecord locationRecord =
                      MAPPER.readValue(locationJson, LocationRecord.class);
                  return locationRecord.getHasCoordinate()
                      && locationRecord.getDecimalLatitude() != null
                      && locationRecord.getDecimalLongitude() != null
                      && locationRecord.getDecimalLatitude() >= -90.0
                      && locationRecord.getDecimalLatitude() <= 90.0
                      && locationRecord.getDecimalLongitude() >= -180.0
                      && locationRecord.getDecimalLongitude() <= 180.0;
                })
        .map(
            (MapFunction<Row, EventCoordinate>)
                row -> {
                  String eventId = row.getAs("eventId");
                  String locationJson = row.getAs("location");
                  LocationRecord locationRecord =
                      MAPPER.readValue(locationJson, LocationRecord.class);
                  return new EventCoordinate(
                      eventId,
                      locationRecord.getDecimalLongitude(),
                      locationRecord.getDecimalLatitude());
                },
            Encoders.bean(EventCoordinate.class));
  }

  private static Dataset<Occurrence> loadOccurrences(
      SparkSession spark, FileSystem fs, String outputPath) throws IOException {

    Encoder<Occurrence> encoder = Encoders.bean(Occurrence.class);
    Path occurrencePath = new Path(outputPath, SIMPLE_OCCURRENCE);
    Path successMarker = new Path(occurrencePath, "_SUCCESS");

    if (fs.exists(occurrencePath) && fs.exists(successMarker)) {
      return spark.read().parquet(occurrencePath.toString()).as(encoder);
    }

    return spark.emptyDataset(encoder);
  }

  private static Dataset<EventCoordinate> getEventCoordinates(Dataset<Event> events) {
    return events
        .map(
            (MapFunction<Event, Tuple3<String, Double, Double>>)
                event -> {
                  LocationRecord lir = MAPPER.readValue(event.getLocation(), LocationRecord.class);
                  return new Tuple3<>(
                      event.getId(), lir.getDecimalLongitude(), lir.getDecimalLatitude());
                },
            Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE(), Encoders.DOUBLE()))
        .filter(
            (FilterFunction<Tuple3<String, Double, Double>>)
                t -> {
                  return (t._2() != null) && (t._3() != null);
                })
        .map(
            (MapFunction<Tuple3<String, Double, Double>, EventCoordinate>)
                t -> {
                  return new EventCoordinate(t._1(), t._2(), t._3());
                },
            Encoders.bean(EventCoordinate.class));
  }

  private static Dataset<Tuple2<String, EventDate>> getCoreIdEventDates(
      Dataset<Occurrence> occurrence) {
    return occurrence
        .map(
            (MapFunction<Occurrence, Tuple2<String, EventDate>>)
                occ -> {
                  TemporalRecord lr = MAPPER.readValue(occ.getTemporal(), TemporalRecord.class);
                  String coreId = occ.getCoreId();
                  return new Tuple2<>(coreId, lr.getEventDate());
                },
            Encoders.tuple(Encoders.STRING(), Encoders.bean(EventDate.class)))
        .distinct();
  }

  private static Dataset<EventCoordinate> getCoreIdCoordinates(Dataset<Occurrence> occurrence) {
    return occurrence
        .map(
            (MapFunction<Occurrence, Tuple3<String, Double, Double>>)
                occ -> {
                  LocationRecord lir = MAPPER.readValue(occ.getLocation(), LocationRecord.class);
                  String coreId = occ.getCoreId();
                  return new Tuple3<>(coreId, lir.getDecimalLongitude(), lir.getDecimalLatitude());
                },
            Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE(), Encoders.DOUBLE()))
        .filter(
            (FilterFunction<Tuple3<String, Double, Double>>)
                t -> {
                  return (t._2() != null) && (t._3() != null);
                })
        .map(
            (MapFunction<Tuple3<String, Double, Double>, EventCoordinate>)
                t -> {
                  return new EventCoordinate(t._1(), t._2(), t._3());
                },
            Encoders.bean(EventCoordinate.class))
        .distinct();
  }

  @Data
  public static class TemporalAccum implements Serializable {

    private String minDate;
    private String maxDate;

    public TemporalAccum acc(EventDate eventDate) {
      Optional.ofNullable(eventDate.getGte()).ifPresent(this::setMinDate);
      Optional.ofNullable(eventDate.getLte()).ifPresent(this::setMaxDate);
      return this;
    }

    private void setMinDate(String date) {
      if (Objects.isNull(minDate)) {
        minDate = date;
      } else {
        minDate =
            StringToDateFunctions.getStringToEarliestEpochSeconds(false)
                        .apply(date)
                        .compareTo(
                            StringToDateFunctions.getStringToEarliestEpochSeconds(false)
                                .apply(minDate))
                    < 0
                ? date
                : minDate;
      }
    }

    private void setMaxDate(String date) {
      if (Objects.isNull(maxDate)) {
        maxDate = date;
      } else {
        maxDate =
            StringToDateFunctions.getStringToLatestEpochSeconds(false)
                        .apply(date)
                        .compareTo(
                            StringToDateFunctions.getStringToLatestEpochSeconds(false)
                                .apply(maxDate))
                    > 0
                ? date
                : maxDate;
      }
    }

    public Optional<EventDate> toEventDate() {
      return Objects.isNull(minDate) && Objects.isNull(maxDate)
          ? Optional.empty()
          : Optional.of(getEventDate());
    }

    private EventDate getEventDate() {
      EventDate.Builder evenDate = EventDate.newBuilder();
      Optional.ofNullable(minDate).ifPresent(evenDate::setGte);
      Optional.ofNullable(maxDate).ifPresent(evenDate::setLte);
      return evenDate.build();
    }
  }
}
