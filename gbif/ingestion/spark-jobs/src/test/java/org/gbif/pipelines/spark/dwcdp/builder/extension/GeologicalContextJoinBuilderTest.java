package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GeologicalContextJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder().appName("GeologicalContextJoinBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> eventDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("eventID", DataTypes.StringType)
            .add("geologicalContextID", DataTypes.StringType)
            .add("eventDate", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> geoContextDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("geologicalContext_pk", DataTypes.StringType)
            .add("geologicalContextID", DataTypes.StringType)
            .add("earliestEonOrLowestEonothem", DataTypes.StringType)
            .add("formation", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- tests ----

  @Test
  void absentGeoContextTable_returnsOriginalDf() {
    Dataset<Row> event = eventDf(List.of(RowFactory.create("EVT001", "GEO-1", "2024-06-01")));

    Dataset<Row> result = GeologicalContextJoinBuilder.enrichEvents(TestTableLoader.of(), event);

    assertEquals(event.columns().length, result.columns().length);
    assertEquals(1L, result.count());
  }

  @Test
  void eventWithoutGeologicalContextIdColumn_returnsOriginalDf() {
    StructType schema =
        new StructType()
            .add("eventID", DataTypes.StringType)
            .add("eventDate", DataTypes.StringType);
    Dataset<Row> event =
        spark.createDataFrame(List.of(RowFactory.create("EVT001", "2024-06-01")), schema);
    Dataset<Row> geoDf =
        geoContextDf(
            List.of(RowFactory.create("GCPK-1", "GEO-1", "Phanerozoic", "Tunbridge Wells Sand")));

    Dataset<Row> result =
        GeologicalContextJoinBuilder.enrichEvents(
            TestTableLoader.of(GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT, geoDf),
            event);

    assertEquals(2, result.columns().length, "should be original columns only");
    assertEquals(1L, result.count());
  }

  @Test
  void joinAddsGeologicalContextColumns() {
    Dataset<Row> event = eventDf(List.of(RowFactory.create("EVT001", "GEO-1", "2024-06-01")));
    Dataset<Row> geoDf =
        geoContextDf(
            List.of(RowFactory.create("GCPK-1", "GEO-1", "Phanerozoic", "Tunbridge Wells Sand")));

    Dataset<Row> result =
        GeologicalContextJoinBuilder.enrichEvents(
            TestTableLoader.of(GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT, geoDf),
            event);

    List<String> cols = Arrays.asList(result.columns());
    assertTrue(cols.contains("earliestEonOrLowestEonothem"));
    assertTrue(cols.contains("formation"));
    assertTrue(cols.contains("eventID"), "eventID must be preserved");

    Row row = result.filter(result.col("eventID").equalTo("EVT001")).first();
    assertEquals("Phanerozoic", row.getAs("earliestEonOrLowestEonothem"));
    assertEquals("Tunbridge Wells Sand", row.getAs("formation"));
  }

  @Test
  void geologicalContextSurrogatePkNeverLeaksIntoResult() {
    Dataset<Row> event = eventDf(List.of(RowFactory.create("EVT001", "GEO-1", "2024-06-01")));
    Dataset<Row> geoDf =
        geoContextDf(List.of(RowFactory.create("GCPK-1", "GEO-1", "Phanerozoic", "Formation X")));

    Dataset<Row> result =
        GeologicalContextJoinBuilder.enrichEvents(
            TestTableLoader.of(GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT, geoDf),
            event);

    assertTrue(
        Arrays.stream(result.columns()).noneMatch(c -> c.equals("geologicalContext_pk")),
        "geological-context's own surrogate PK must never appear on event rows");
  }

  @Test
  void eventWithNoMatchingGeologicalContext_survivesWithNullFields() {
    Dataset<Row> event = eventDf(List.of(RowFactory.create("EVT001", "GEO-UNKNOWN", "2024-06-01")));
    Dataset<Row> geoDf =
        geoContextDf(List.of(RowFactory.create("GCPK-1", "GEO-1", "Phanerozoic", "Formation X")));

    Dataset<Row> result =
        GeologicalContextJoinBuilder.enrichEvents(
            TestTableLoader.of(GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT, geoDf),
            event);

    assertEquals(1L, result.count(), "event row must survive left join even with no match");
    Row row = result.first();
    assertTrue(row.isNullAt(row.fieldIndex("earliestEonOrLowestEonothem")));
  }

  @Test
  void manyEventsToOneGeologicalContext_allRowsGetContextFields() {
    Dataset<Row> event =
        eventDf(
            List.of(
                RowFactory.create("EVT001", "GEO-1", "2024-06-01"),
                RowFactory.create("EVT002", "GEO-1", "2024-06-02")));
    Dataset<Row> geoDf =
        geoContextDf(List.of(RowFactory.create("GCPK-1", "GEO-1", "Phanerozoic", "Formation X")));

    Dataset<Row> result =
        GeologicalContextJoinBuilder.enrichEvents(
            TestTableLoader.of(GeologicalContextJoinBuilder.TABLE_GEOLOGICAL_CONTEXT, geoDf),
            event);

    assertEquals(2L, result.count());
    assertEquals(
        1L,
        result.select("formation").distinct().count(),
        "both events should carry the same geological context");
  }

  @Test
  void joinDoesNotDuplicateColumnsAlreadyOnEvent() {
    Dataset<Row> event = eventDf(List.of(RowFactory.create("EVT001", "GEO-1", "2024-06-01")));
    Dataset<Row> geoDf =
        geoContextDf(List.of(RowFactory.create("GCPK-1", "GEO-1", "Phanerozoic", "Formation X")));

    Dataset<Row> result = GeologicalContextJoinBuilder.join(event, geoDf);

    long distinctColCount = Arrays.stream(result.columns()).distinct().count();
    assertEquals(
        result.columns().length, distinctColCount, "result must have no duplicate column names");
  }
}
