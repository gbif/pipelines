package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;
import org.gbif.pipelines.spark.pojo.Event;
import org.gbif.pipelines.spark.util.EventInheritanceUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class EventInheritanceTest {

  private static SparkSession spark;
  private static java.nio.file.Path sparkTmp = null;

  @BeforeClass
  public static void setUp() throws Exception {
    sparkTmp = Files.createTempDirectory("spark-local");
    spark =
        SparkSession.builder()
            .master("local[1]")
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.local.dir", sparkTmp.toAbsolutePath().toString())
            .getOrCreate();
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void testInheritance() throws Exception {

    final ObjectMapper MAPPER = new ObjectMapper();
    Event parentEvent =
        Event.builder()
            .id("parent-event-id-1")
            .lineage(List.of())
            .eventCore(
                MAPPER.writeValueAsString(
                    EventCoreRecord.newBuilder()
                        .setId("parent-event-id-1")
                        .setEventType(VocabularyConcept.newBuilder().setConcept("Project").build())
                        .setLocationID("location-id-1")
                        .build()))
            .location(
                MAPPER.writeValueAsString(
                    LocationRecord.newBuilder()
                        .setId("parent-event-id-1")
                        .setCountryCode("KE")
                        .setBiome("Biome")
                        .setContinent("Africa")
                        .setDecimalLatitude(-13.2921)
                        .setDecimalLongitude(12.2921)
                        .build()))
            .temporal(
                MAPPER.writeValueAsString(
                    TemporalRecord.newBuilder()
                        .setId("parent-event-id-1")
                        .setEventDate(
                            EventDate.newBuilder()
                                .setLte("2020-12-01T00:00:00Z")
                                .setGte("2020-12-01T23:59:59Z")
                                .build())
                        .setYear(2020)
                        .setMonth(12)
                        .setDay(1)
                        .build()))
            .build();

    Event childEvent =
        Event.builder()
            .id("child-event-id-1")
            .lineage(List.of("parent-event-id-1"))
            .eventCore(
                MAPPER.writeValueAsString(
                    EventCoreRecord.newBuilder()
                        .setId("child-event-id-1")
                        .setParentEventID("parent-event-id-1")
                        .setEventType(
                            VocabularyConcept.newBuilder().setConcept("SiteVisit").build())
                        .build()))
            .location(
                MAPPER.writeValueAsString(
                    LocationRecord.newBuilder().setId("child-event-id-1").build()))
            .temporal(
                MAPPER.writeValueAsString(
                    TemporalRecord.newBuilder().setId("child-event-id-1").build()))
            .build();

    Event granChildEvent =
        Event.builder()
            .id("gran-child-event-id-1")
            .lineage(List.of("parent-event-id-1", "child-event-id-1"))
            .eventCore(
                MAPPER.writeValueAsString(
                    EventCoreRecord.newBuilder()
                        .setId("gran-child-event-id-1")
                        .setParentEventID("child-event-id-1")
                        .setEventType(VocabularyConcept.newBuilder().setConcept("Sample").build())
                        .build()))
            .location(
                MAPPER.writeValueAsString(
                    LocationRecord.newBuilder().setId("gran-child-event-id-1").build()))
            .temporal(
                MAPPER.writeValueAsString(
                    TemporalRecord.newBuilder().setId("gran-child-event-id-1").build()))
            .build();

    // create working area for test
    URL testRootUrl = EventInheritanceTest.class.getResource("/");
    assert testRootUrl != null;
    String testResourcesRoot = testRootUrl.getFile();

    // create dataframe
    List<Event> events = List.of(parentEvent, childEvent, granChildEvent);
    Dataset<Row> eventsDf = spark.createDataFrame(events, Event.class);
    assertEquals(3L, eventsDf.count());

    String inheritanceInput = testResourcesRoot + "/events-inheritance/" + Directories.SIMPLE_EVENT;

    // write to disk
    eventsDf.write().mode("overwrite").parquet(inheritanceInput);

    TestConfigUtil.createConfigYaml(
        testResourcesRoot,
        testResourcesRoot,
        "zk_not_used", // not used for test
        9999, // not used for test
        9999, // not used for test
        9999, // not used for test
        9999, // not used for test
        "pipelines.yaml");

    // create a dataframe with the list of objects
    PipelinesConfig config = loadConfig(testResourcesRoot + "pipelines.yaml");
    SparkSession spark =
        getSparkSession(
            "local[1]", "Event", config, OccurrenceInterpretationPipeline::configSparkSession);

    // run inheritance
    List<Event> enrichedEvents =
        EventInheritanceUtil.runEventInheritance(spark, testResourcesRoot + "/events-inheritance/")
            .collectAsList();

    for (Event event : enrichedEvents) {
      // validate child has inherited
      if (event.getId().equals("child-event-id-1")) {

        // check the child has inherited from both parent
        EventInheritedRecord eventInheritedRecord =
            MAPPER.readValue(event.getEventInherited(), EventInheritedRecord.class);
        assertEquals("location-id-1", eventInheritedRecord.getLocationID());
        LocationInheritedRecord locationInheritedRecord =
            MAPPER.readValue(event.getLocationInherited(), LocationInheritedRecord.class);
        assertEquals("KE", locationInheritedRecord.getCountryCode());
        TemporalInheritedRecord temporalInheritedRecord =
            MAPPER.readValue(event.getTemporalInherited(), TemporalInheritedRecord.class);
        assertEquals(2020, (int) temporalInheritedRecord.getYear());
      } else if (event.getId().equals("gran-child-event-id-1")) {

        // check the granchild has inherited from both parent and child
        EventInheritedRecord eventInheritedRecord =
            MAPPER.readValue(event.getEventInherited(), EventInheritedRecord.class);
        assertEquals("location-id-1", eventInheritedRecord.getLocationID());
        LocationInheritedRecord locationInheritedRecord =
            MAPPER.readValue(event.getLocationInherited(), LocationInheritedRecord.class);
        assertEquals("KE", locationInheritedRecord.getCountryCode());
        TemporalInheritedRecord temporalInheritedRecord =
            MAPPER.readValue(event.getTemporalInherited(), TemporalInheritedRecord.class);
        assertEquals(2020, (int) temporalInheritedRecord.getYear());

      } else if (event.getId().equals("parent-event-id-1")) {
        // do nothing
      } else {
        throw new AssertionError("Unknown event id: " + event.getId());
      }
    }
  }
}
