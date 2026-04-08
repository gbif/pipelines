package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.io.avro.Parent;
import org.gbif.pipelines.spark.pojo.EventLineage;
import org.gbif.pipelines.spark.util.LineageUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple3;

public class CalculateLineageTest {

  private static SparkSession spark;
  private static Path sparkTmp = null;

  @BeforeClass
  public static void setUp() throws Exception {
    sparkTmp = Files.createTempDirectory("spark-local");
    spark =
        SparkSession.builder()
            .master("local[1]")
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
  public void testCalculateLineage() throws Exception {

    java.util.List<Tuple3<String, String, String>> events =
        java.util.List.of(
            new Tuple3<>("Event1", "TypeA", null),
            new Tuple3<>("Event2", "TypeB", "Event1"),
            new Tuple3<>("Event3", "TypeC", "Event1"),
            new Tuple3<>("Event4", "TypeD", "Event2"));

    Dataset<Row> eventDf =
        spark
            .createDataset(
                events, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()))
            .select(
                col("_1").as("eventId"), col("_2").as("eventType"), col("_3").as("parentEventId"))
            .toDF();
    Dataset<EventLineage> lineagesDf = LineageUtil.calculateLineage(spark, eventDf);

    // convert to list
    List<EventLineage> lineages = lineagesDf.collectAsList();

    // Expect one lineage row per input event
    assertEquals(4, lineages.size());

    // Each event id should appear in the output JSON
    assertTrue(lineages.stream().anyMatch(j -> j.getId().equals("Event1")));
    assertTrue(lineages.stream().anyMatch(j -> j.getId().equals("Event2")));
    assertTrue(lineages.stream().anyMatch(j -> j.getId().equals("Event3")));
    assertTrue(lineages.stream().anyMatch(j -> j.getId().equals("Event4")));

    // assert Event4 has parents Event2 and Event1
    EventLineage event4 =
        lineages.stream().filter(l -> "Event4".equals(l.getId())).findFirst().orElse(null);
    assertNotNull(event4);

    List<Parent> parents = event4.getLineage();
    assertTrue(parents != null && parents.size() >= 2);

    // presence checks
    assertTrue(parents.stream().anyMatch(p -> "Event2".equals(p.getId())));
    assertTrue(parents.stream().anyMatch(p -> "Event1".equals(p.getId())));

    // assert Event3 has parent Event1
    EventLineage event3 =
        lineages.stream().filter(l -> "Event3".equals(l.getId())).findFirst().orElse(null);
    assertNotNull(event3);

    // assert Event3 does not have parent  Event2
    List<Parent> parents3 = event3.getLineage();
    assertTrue(parents3 != null && parents3.size() == 2);
    assertTrue(parents3.stream().anyMatch(p -> "Event1".equals(p.getId())));
    assertTrue(parents3.stream().anyMatch(p -> "Event3".equals(p.getId())));
  }
}
