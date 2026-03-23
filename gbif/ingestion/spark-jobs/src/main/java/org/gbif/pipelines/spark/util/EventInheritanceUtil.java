package org.gbif.pipelines.spark.util;

import static org.gbif.pipelines.spark.Directories.*;
import static org.gbif.pipelines.spark.EventInterpretationPipeline.sparkLog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;
import org.gbif.pipelines.spark.pojo.Event;
import org.gbif.pipelines.spark.pojo.EventInheritedFields;
import scala.Tuple2;
import scala.collection.mutable.ListBuffer;

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

    log.info("Creating temp view Event");

    sparkLog(spark, "event-interpretation", "Creating temp view Event");

    // load simple event
    Dataset<Event> events =
        spark.read().parquet(inputPath + "/" + SIMPLE_EVENT).as(Encoders.bean(Event.class));
    events.createOrReplaceTempView("simple_event");

    log.info("Created temp view Event count: {}", events.count());

    sparkLog(spark, "event-interpretation", "Joining to parents to get inherited info");

    // Explode the lineage array to produce one row per parent id (use OUTER explode to
    // preserve events with no parents). This avoids using array_contains in the join
    // predicate which can cause heavy shuffles and OOMs.
    Dataset<Row> joinedToParents =
        spark.sql(
            """
            SELECT
              e.id as id,
              e.eventCore as eventCore,
              e.location as location,
              e.temporal as temporal,
              pe.eventCore as eventCoreInfo,
              pe.location as locationInfo,
              pe.temporal as temporalInfo
            FROM (
              SELECT
                se.id as id,
                se.eventCore as eventCore,
                se.location as location,
                se.temporal as temporal,
                parent_id
              FROM simple_event se
              LATERAL VIEW OUTER explode(se.lineage) as parent_id
            ) e
            LEFT OUTER JOIN simple_event pe
              ON e.parent_id = pe.id
        """);

    log.info("Joined to parents: {}", joinedToParents.count());

    sparkLog(spark, "event-interpretation", "Consolidating...");
    ObjectMapper mapper = new ObjectMapper();

    // one row per id
    // Use groupByKey + mapGroups to stream rows per group and build parent lists incrementally
    // This avoids collect_list over the whole shuffle which can cause OOM for large groups
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.StringType, true),
              DataTypes.createStructField("eventCore", DataTypes.StringType, true),
              DataTypes.createStructField("location", DataTypes.StringType, true),
              DataTypes.createStructField("temporal", DataTypes.StringType, true),
              DataTypes.createStructField(
                  "eventCoreInfos", DataTypes.createArrayType(DataTypes.StringType), true),
              DataTypes.createStructField(
                  "locationInfos", DataTypes.createArrayType(DataTypes.StringType), true),
              DataTypes.createStructField(
                  "temporalInfos", DataTypes.createArrayType(DataTypes.StringType), true)
            });

    Dataset<Row> consolidated =
        joinedToParents
            .groupByKey((MapFunction<Row, String>) r -> r.getAs("id"), Encoders.STRING())
            .mapGroups(
                new org.apache.spark.api.java.function.MapGroupsFunction<String, Row, Row>() {
                  @Override
                  public Row call(String id, java.util.Iterator<Row> iter) {
                    String eventCore = null;
                    String location = null;
                    String temporal = null;

                    ListBuffer<String> eventCoreInfos = new ListBuffer<String>();
                    ListBuffer<String> locationInfos = new ListBuffer<String>();
                    ListBuffer<String> temporalInfos = new ListBuffer<String>();

                    while (iter.hasNext()) {
                      Row r = iter.next();

                      if (eventCore == null) {
                        eventCore = r.getAs("eventCore");
                      }
                      if (location == null) {
                        location = r.getAs("location");
                      }
                      if (temporal == null) {
                        temporal = r.getAs("temporal");
                      }

                      String eci = r.getAs("eventCoreInfo");
                      if (eci != null) {
                        eventCoreInfos.$plus$eq(eci);
                      }
                      String li = r.getAs("locationInfo");
                      if (li != null) {
                        locationInfos.$plus$eq(li);
                      }
                      String ti = r.getAs("temporalInfo");
                      if (ti != null) {
                        temporalInfos.$plus$eq(ti);
                      }
                    }

                    return RowFactory.create(
                        id,
                        eventCore,
                        location,
                        temporal,
                        eventCoreInfos.toSeq(),
                        locationInfos.toSeq(),
                        temporalInfos.toSeq());
                  }
                },
                Encoders.row(schema));

    log.info("Events consolidated: {}", consolidated.count());

    sparkLog(spark, "event-interpretation", "eventsWithInheritedInfo...");
    Dataset<EventInheritedFields> eventsWithInheritedInfo =
        consolidated.map(
            (MapFunction<Row, EventInheritedFields>)
                row -> {
                  String coreId = row.getAs(0);
                  EventCoreRecord eventCore =
                      mapper.readValue((String) row.getAs(1), EventCoreRecord.class);
                  LocationRecord location =
                      mapper.readValue((String) row.getAs(2), LocationRecord.class);
                  TemporalRecord temporal =
                      mapper.readValue((String) row.getAs(3), TemporalRecord.class);

                  List<EventCoreRecord> eventCoreFromParents =
                      (row.getList(4))
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
                      (row.getList(5))
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
                      (row.getList(6))
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
                      .id(coreId)
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

    log.info(
        "Event inheritance process finished. Output written to "
            + inputPath
            + "/"
            + SIMPLE_EVENT_WITH_INHERITED);

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
