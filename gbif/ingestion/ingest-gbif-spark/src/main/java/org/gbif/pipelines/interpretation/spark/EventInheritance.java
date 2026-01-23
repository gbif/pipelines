package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;
import static org.gbif.pipelines.interpretation.spark.Directories.*;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getSparkSession;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;
import scala.Tuple2;

@Slf4j
public class EventInheritance {

  public static void main(String[] args) throws Exception {

    PipelinesConfig config =
        loadConfig(
            "/Users/djtfmartin/dev/my-forks/pipelines/gbif/ingestion/ingest-gbif-spark/pipelines.yaml");
    String datasetId = "ecebee66-f913-4105-acb6-738430d0edc9";
    int attempt = 1;

    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    SparkSession spark =
        getSparkSession("local[*]", "My app", config, Interpretation::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    runEventInheritance(spark, outputPath);

    fileSystem.close();
    spark.stop();
    spark.close();
    System.exit(0);
  }

  public static Dataset<Event> runEventInheritance(SparkSession spark, String outputPath) {

    // load simple event
    Dataset<Event> events =
        spark.read().parquet(outputPath + "/" + SIMPLE_EVENT).as(Encoders.bean(Event.class));

    events.createOrReplaceTempView("simple_event");

    Dataset<Row> joinedToParents =
        spark.sql(
            """
            SELECT
                   se.eventCore as eventCore,
                   se.location as location,
                   se.temporal as temporal,
                   pe.eventCore as eventCoreInfo,
                   pe.location as locationInfo,
                   pe.temporal as temporalInfo
            FROM simple_event se
            LEFT OUTER JOIN simple_event pe
            ON array_contains(se.lineage, pe.id)
        """);

    ObjectMapper mapper = new ObjectMapper();

    Dataset<EventInheritedFields> eventsWithInheritedInfo =
        joinedToParents.map(
            (MapFunction<Row, EventInheritedFields>)
                row -> {

                  // main info
                  EventCoreRecord eventCore =
                      mapper.readValue((String) row.getAs("eventCore"), EventCoreRecord.class);
                  LocationRecord location =
                      mapper.readValue((String) row.getAs("location"), LocationRecord.class);
                  TemporalRecord temporal =
                      mapper.readValue((String) row.getAs("temporal"), TemporalRecord.class);

                  // parent info
                  List<EventCoreRecord> eventCoreFromParents =
                      deserializeFieldToList(row, "eventCoreInfo", mapper, EventCoreRecord.class);
                  List<LocationRecord> locationsFromParents =
                      deserializeFieldToList(row, "locationInfo", mapper, LocationRecord.class);
                  List<TemporalRecord> temporalFromParents =
                      deserializeFieldToList(row, "temporalInfo", mapper, TemporalRecord.class);

                  EventInheritedRecord eventInheritedFields =
                      inheritEventFrom(eventCore, eventCoreFromParents);
                  LocationInheritedRecord locationInheritedRecord =
                      inheritLocationFrom(location, locationsFromParents);
                  TemporalInheritedRecord temporalInheritedRecord =
                      inheritTemporalFrom(temporal, temporalFromParents);

                  return EventInheritedFields.builder()
                      .id(eventCore.getId())
                      .eventInherited(mapper.writeValueAsString(eventInheritedFields))
                      .locationInherited(mapper.writeValueAsString(locationInheritedRecord))
                      .temporalInherited(mapper.writeValueAsString(temporalInheritedRecord))
                      .build();
                },
            Encoders.bean(EventInheritedFields.class));

    eventsWithInheritedInfo
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/" + EVENT_INHERITED_FIELDS);

    // join back to events and write final output if needed
    events
        .joinWith(
            eventsWithInheritedInfo,
            events.col("id").equalTo(eventsWithInheritedInfo.col("id")),
            "left_outer")
        .map(
            (MapFunction<Tuple2<Event, EventInheritedFields>, Event>)
                t -> {
                  Event event = t._1;
                  EventInheritedFields inherited = t._2;
                  if (inherited != null) {
                    event.setEventInherited(inherited.getEventInherited());
                    event.setLocationInherited(inherited.getLocationInherited());
                    event.setTemporalInherited(inherited.getTemporalInherited());
                  }
                  return event;
                },
            Encoders.bean(Event.class))
        .write()
        .mode("overwrite")
        .parquet(outputPath + "/" + SIMPLE_EVENT);

    log.info("Event inheritance process finished");

    return spark.read().parquet(outputPath + "/" + SIMPLE_EVENT).as(Encoders.bean(Event.class));
  }

  private static EventInheritedRecord inheritEventFrom(
      EventCoreRecord child, List<EventCoreRecord> parents) {

    EventInheritedRecord record =
        EventInheritedRecord.newBuilder()
            .setId(child.getId())
            .setLocationID(child.getLocationID())
            .setEventType(
                child.getParentsLineage().stream()
                    .map(Parent::getEventType)
                    .filter(Objects::nonNull)
                    .toList())
            .build();

    for (EventCoreRecord parent : parents) {
      if (record.getLocationID() == null && parent.getLocationID() != null) {
        record.setLocationID(parent.getLocationID());
      }
    }

    return record;
  }

  private static LocationInheritedRecord inheritLocationFrom(
      LocationRecord child, List<LocationRecord> parents) {

    LocationInheritedRecord record =
        LocationInheritedRecord.newBuilder()
            .setId(child.getId())
            .setCountryCode(child.getCountryCode())
            .setStateProvince(child.getStateProvince())
            .setDecimalLatitude(child.getDecimalLatitude())
            .setDecimalLongitude(child.getDecimalLongitude())
            .build();

    for (LocationRecord parent : parents) {
      if (record.getCountryCode() == null && parent.getCountryCode() != null) {
        record.setCountryCode(parent.getCountryCode());
        record.setInheritedFrom(parent.getId());
      }
      if (record.getStateProvince() == null && parent.getStateProvince() != null) {
        record.setStateProvince(parent.getStateProvince());
        record.setInheritedFrom(parent.getId());
      }
      if (record.getDecimalLatitude() == null && parent.getDecimalLatitude() != null) {
        record.setDecimalLatitude(parent.getDecimalLatitude());
        record.setInheritedFrom(parent.getId());
      }
      if (record.getDecimalLongitude() == null && parent.getDecimalLongitude() != null) {
        record.setDecimalLongitude(parent.getDecimalLongitude());
        record.setInheritedFrom(parent.getId());
      }
    }

    return record;
  }

  private static TemporalInheritedRecord inheritTemporalFrom(
      TemporalRecord child, List<TemporalRecord> parents) {

    TemporalInheritedRecord record =
        TemporalInheritedRecord.newBuilder()
            .setId(child.getId())
            .setYear(child.getYear())
            .setMonth(child.getMonth())
            .setDay(child.getDay())
            .build();
    for (TemporalRecord parent : parents) {
      if (record.getYear() == null && parent.getYear() != null) {
        record.setYear(parent.getYear());
        record.setInheritedFrom(parent.getId());
      }
      if (record.getMonth() == null && parent.getMonth() != null) {
        record.setMonth(parent.getMonth());
        record.setInheritedFrom(parent.getId());
      }
      if (record.getDay() == null && parent.getDay() != null) {
        record.setDay(parent.getDay());
        record.setInheritedFrom(parent.getId());
      }
    }

    return record;
  }

  private static <T> List<T> deserializeFieldToList(
      Row row, String field, ObjectMapper mapper, Class<T> clazz) throws JsonProcessingException {
    String fieldValue = row.getAs(field);
    List<T> results;

    if (fieldValue == null) {
      results = Collections.emptyList();
    } else {
      fieldValue = fieldValue.trim();

      // If it starts with "{", it's a single object → wrap into array
      if (fieldValue.startsWith("{")) {
        fieldValue = "[" + fieldValue + "]";
      }

      JavaType listType = mapper.getTypeFactory().constructCollectionType(List.class, clazz);

      results = mapper.readValue(fieldValue, listType);
    }
    return results;
  }
}
