package org.gbif.pipelines.spark.util;

import static org.gbif.pipelines.spark.Directories.*;
import static org.gbif.pipelines.spark.EventInterpretationPipeline.sparkLog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
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
 * and outputs a new parquet dataset containing those inherited fields as typed structs.
 *
 * <p>Inherited fields include: eventCore.locationID and the parents' event types, location
 * (countryCode, stateProvince, decimalLatitude, decimalLongitude) and temporal (year, month, day).
 */
@Slf4j
public class EventInheritanceUtil {

  /**
   * Reads simple event parquet, calculates location, eventcore and temporal fields that should be
   * inherited from parents where available.
   *
   * <p>This uses the pre-calculated lineage field to join events to their parents in order to
   * perform the inheritance logic.
   *
   * @param spark the spark session
   * @param inputPath the parquet to read and process with
   * @return dataframe of Event objects with enriched event data
   */
  public static Dataset<Event> runEventInheritance(SparkSession spark, String inputPath) {

    final Encoder<Event> eventEncoder = Encoders.bean(Event.class);

    sparkLog(spark, "event-interpretation", "Loading events for inheritance");

    // load simple event, keeping only the fields inheritance needs to keep the shuffle small
    Dataset<Event> events =
        spark
            .read()
            .parquet(inputPath + "/" + SIMPLE_EVENT)
            .as(eventEncoder)
            .map(
                (MapFunction<Event, Event>)
                    e ->
                        Event.builder()
                            .id(e.getId())
                            .lineage(e.getLineage())
                            .eventCore(e.getEventCore())
                            .location(e.getLocation())
                            .temporal(e.getTemporal())
                            .build(),
                eventEncoder);

    sparkLog(spark, "event-interpretation", "Joining to parents to get inherited info");

    // One row per (event, parent id). Events with no parents keep a single row with a null parent
    // id. This avoids using array_contains in the join predicate which can cause heavy shuffles
    // and OOMs.
    Dataset<Tuple2<Event, String>> eventToParentId =
        events.flatMap(
            (FlatMapFunction<Event, Tuple2<Event, String>>)
                e -> {
                  List<String> lineage = e.getLineage();
                  if (lineage == null || lineage.isEmpty()) {
                    return Collections.singletonList(new Tuple2<Event, String>(e, null)).iterator();
                  }
                  List<Tuple2<Event, String>> rows = new ArrayList<>(lineage.size());
                  for (String parentId : lineage) {
                    rows.add(new Tuple2<>(e, parentId));
                  }
                  return rows.iterator();
                },
            Encoders.tuple(eventEncoder, Encoders.STRING()));

    Dataset<Event> parents = events.alias("parent");
    Dataset<Tuple2<Tuple2<Event, String>, Event>> joinedToParents =
        eventToParentId.joinWith(
            parents, eventToParentId.col("_2").equalTo(parents.col("id")), "left_outer");

    sparkLog(spark, "event-interpretation", "Consolidating...");

    // One row per id. Use groupByKey + mapGroups to stream rows per group and build parent lists
    // incrementally. This avoids collect_list over the whole shuffle which can cause OOM for large
    // groups.
    Dataset<EventInheritedFields> eventsWithInheritedInfo =
        joinedToParents
            .groupByKey(
                (MapFunction<Tuple2<Tuple2<Event, String>, Event>, String>)
                    t -> t._1()._1().getId(),
                Encoders.STRING())
            .mapGroups(
                (MapGroupsFunction<
                        String, Tuple2<Tuple2<Event, String>, Event>, EventInheritedFields>)
                    (id, iter) -> consolidate(id, iter),
                Encoders.bean(EventInheritedFields.class));

    eventsWithInheritedInfo
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(inputPath + "/" + EVENT_INHERITED_FIELDS);

    // Clear the catalog cache to invalidate any stale file metadata
    spark.catalog().clearCache();

    // Reload events from disk
    Dataset<Event> freshEvents =
        spark.read().parquet(inputPath + "/" + SIMPLE_EVENT).as(eventEncoder);

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
            eventEncoder)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(inputPath + "/" + SIMPLE_EVENT_WITH_INHERITED);

    log.info(
        "Event inheritance process finished. Output written to "
            + inputPath
            + "/"
            + SIMPLE_EVENT_WITH_INHERITED);

    return spark.read().parquet(inputPath + "/" + SIMPLE_EVENT_WITH_INHERITED).as(eventEncoder);
  }

  /** Builds the inherited fields for one event from the event and all its parents. */
  private static EventInheritedFields consolidate(
      String id, Iterator<Tuple2<Tuple2<Event, String>, Event>> iter) {
    Event child = null;
    List<EventCoreRecord> eventCoreFromParents = new ArrayList<>();
    List<LocationRecord> locationsFromParents = new ArrayList<>();
    List<TemporalRecord> temporalFromParents = new ArrayList<>();

    while (iter.hasNext()) {
      Tuple2<Tuple2<Event, String>, Event> row = iter.next();
      if (child == null) {
        child = row._1()._1();
      }
      Event parent = row._2();
      if (parent != null) {
        if (parent.getEventCore() != null) {
          eventCoreFromParents.add(parent.getEventCore());
        }
        if (parent.getLocation() != null) {
          locationsFromParents.add(parent.getLocation());
        }
        if (parent.getTemporal() != null) {
          temporalFromParents.add(parent.getTemporal());
        }
      }
    }

    return EventInheritedFields.builder()
        .id(id)
        .eventInherited(inheritEventFrom(child.getEventCore(), eventCoreFromParents))
        .locationInherited(inheritLocationFrom(child.getLocation(), locationsFromParents))
        .temporalInherited(inheritTemporalFrom(child.getTemporal(), temporalFromParents))
        .build();
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
}
