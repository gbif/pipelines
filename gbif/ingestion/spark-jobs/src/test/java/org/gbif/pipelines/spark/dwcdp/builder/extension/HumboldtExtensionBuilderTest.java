package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
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

/**
 * Tests for {@link HumboldtExtensionBuilder}.
 *
 * <p>Minimal coverage for now — this builder previously had none at all. Starts with the
 * missing-{@code eventID}-column regression (see {@code EventCoreBuilderTest} for the equivalent on
 * the core path, and the sibling regressions in {@code MediaExtensionBuilderTest}, {@code
 * AssertionExtensionBuilderTest}, {@code IdentifierExtensionBuilderTest}) plus one basic happy-path
 * test; broader coverage (survey-target fan-out, protocol resolution, etc.) is a follow-up.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HumboldtExtensionBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("HumboldtExtensionBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  private Dataset<Row> eventPkDf(List<Row> rows) {
    StructType schema =
        new StructType().add("event_pk", DataTypes.StringType).add("eventID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> surveyDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("survey_pk", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("siteCount", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  @Test
  void surveyResolvesToEventId_carriesSiteCount() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> surveyDf = surveyDf(List.of(RowFactory.create("SPK-001", "EPK-001", "3")));

    Optional<Dataset<Row>> resultOpt =
        HumboldtExtensionBuilder.build(
            spark,
            TestTableLoader.of("event", eventDf, HumboldtExtensionBuilder.TABLE_SURVEY, surveyDf));

    assertTrue(resultOpt.isPresent());
    Row row = resultOpt.get().collectAsList().get(0);
    assertEquals("EVT001", row.getAs("eventID"));
  }

  @Test
  void surveyTableAbsent_returnsEmpty() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));

    Optional<Dataset<Row>> resultOpt =
        HumboldtExtensionBuilder.build(spark, TestTableLoader.of("event", eventDf));

    assertTrue(resultOpt.isEmpty());
  }

  @Test
  void eventTableWithNoEventIdColumn_doesNotThrow() {
    // eventID has no `required: true` constraint in the DwC-DP profile — a package that never
    // populated it can legitimately arrive with the column absent entirely.
    StructType schema = new StructType().add("event_pk", DataTypes.StringType);
    Dataset<Row> eventDf = spark.createDataFrame(List.of(RowFactory.create("EPK-001")), schema);
    Dataset<Row> surveyDf = surveyDf(List.of(RowFactory.create("SPK-001", "EPK-001", "3")));

    Optional<Dataset<Row>> result =
        assertDoesNotThrow(
            () ->
                HumboldtExtensionBuilder.build(
                    spark,
                    TestTableLoader.of(
                        "event", eventDf, HumboldtExtensionBuilder.TABLE_SURVEY, surveyDf)),
            "a missing eventID column must not crash the Humboldt extension");

    assertTrue(result.isEmpty());
  }
}
