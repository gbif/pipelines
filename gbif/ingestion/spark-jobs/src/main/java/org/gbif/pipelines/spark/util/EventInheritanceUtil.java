package org.gbif.pipelines.spark.util;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.expr;
import static org.gbif.pipelines.spark.Directories.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;
import org.gbif.pipelines.spark.pojo.Event;
import org.gbif.pipelines.spark.pojo.EventInheritedFields;
import scala.Tuple2;

/**
 * Utilities for enriching each event with inherited parent-derived event/location/temporal values
 * and outputs a new parquet dataset containing those inherited JSON fields.
 *
 * <p>Inherited fields include: - eventCore.eventType (inherited from parents' eventCore.eventType)
 * - location.d
 */
@Slf4j
public class EventInheritanceUtil {

  /**
   * Reads simple event serialised parquet, calculates location, eventcore and temporal fields that
   * should be inherited from parents where available.
   *
   * <p>This uses the pre-calculated lineage field to join events to their parents in order to
   * perform the inheritance logic.
   *
   * @param spark the spark session
   * @param inputPath the parquet to read and process with
   * @return dataframe of Event objects with enriched event data
   */
  public static Dataset<Event> runEventInheritance(SparkSession spark, String inputPath) {

    // load simple event
    Dataset<Event> events =
        spark.read().parquet(inputPath + "/" + SIMPLE_EVENT).as(Encoders.bean(Event.class));

    events.createOrReplaceTempView("simple_event");

    Dataset<Row> joinedToParents =
        spark.sql(
            """
            SELECT
                  se.id as id,
                   se.eventCore as eventCore,
                   se.location as location,
                   se.temporal as temporal,
                   pe.eventCore as eventCoreInfo,
                   pe.location as locationInfo,
                   pe.temporal as temporalInfo
            FROM simple_event se
            LEFT OUTER JOIN simple_event pe
            ON array_contains(se.lineage, pe.id)
            ORDER BY se.id
        """);

    ObjectMapper mapper = new ObjectMapper();

    // one row per id
    Dataset<Row> consolidated =
        joinedToParents
            .groupBy(col("id"))
            .agg(
                first(col("eventCore"), true).as("eventCore"),
                first(col("location"), true).as("location"),
                first(col("temporal"), true).as("temporal"),
                expr("filter(collect_list(eventCoreInfo), x -> x is not null)")
                    .as("eventCoreInfos"),
                expr("filter(collect_list(locationInfo), x -> x is not null)").as("locationInfos"),
                expr("filter(collect_list(temporalInfo), x -> x is not null)").as("temporalInfos"));

    Dataset<EventInheritedFields> eventsWithInheritedInfo =
        consolidated.map(
            (MapFunction<Row, EventInheritedFields>)
                row -> {
                  EventCoreRecord eventCore =
                      mapper.readValue((String) row.getAs("eventCore"), EventCoreRecord.class);
                  LocationRecord location =
                      mapper.readValue((String) row.getAs("location"), LocationRecord.class);
                  TemporalRecord temporal =
                      mapper.readValue((String) row.getAs("temporal"), TemporalRecord.class);

                  List<EventCoreRecord> eventCoreFromParents =
                      (row.getList(row.fieldIndex("eventCoreInfos")))
                          .stream()
                              .map(
                                  s -> {
                                    try {
                                      return mapper.readValue((String) s, EventCoreRecord.class);
                                    } catch (Exception e) {
                                      throw new RuntimeException(e);
                                    }
                                  })
                              .toList();

                  List<LocationRecord> locationsFromParents =
                      (row.getList(row.fieldIndex("locationInfos")))
                          .stream()
                              .map(
                                  s -> {
                                    try {
                                      return mapper.readValue((String) s, LocationRecord.class);
                                    } catch (Exception e) {
                                      throw new RuntimeException(e);
                                    }
                                  })
                              .toList();

                  List<TemporalRecord> temporalFromParents =
                      (row.getList(row.fieldIndex("temporalInfos")))
                          .stream()
                              .map(
                                  s -> {
                                    try {
                                      return mapper.readValue((String) s, TemporalRecord.class);
                                    } catch (Exception e) {
                                      throw new RuntimeException(e);
                                    }
                                  })
                              .toList();

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
        .parquet(inputPath + "/" + EVENT_INHERITED_FIELDS);

    // Drop the temp view to release cached resources and file metadata
    spark.catalog().dropTempView("simple_event");

    // Clear the catalog cache to invalidate any stale file metadata
    spark.catalog().clearCache();

    // Reload events from disk
    Dataset<Event> freshEvents =
        spark.read().parquet(inputPath + "/" + SIMPLE_EVENT).as(Encoders.bean(Event.class));

    // Reload inherited fields from disk
    Dataset<EventInheritedFields> freshInheritedInfo =
        spark
            .read()
            .parquet(inputPath + "/" + EVENT_INHERITED_FIELDS)
            .as(Encoders.bean(EventInheritedFields.class));

    // join back to events and write to a NEW directory (not the same as input)
    // This avoids the read-write conflict that causes SparkFileNotFoundException
    freshEvents
        .joinWith(
            freshInheritedInfo,
            freshEvents.col("id").equalTo(freshInheritedInfo.col("id")),
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
        .mode(SaveMode.Overwrite)
        .parquet(inputPath + "/" + SIMPLE_EVENT_WITH_INHERITED);

    log.info("Event inheritance process finished");

    return spark
        .read()
        .parquet(inputPath + "/" + SIMPLE_EVENT_WITH_INHERITED)
        .as(Encoders.bean(Event.class));
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
        record.setInheritedFrom(parent.getId());
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
