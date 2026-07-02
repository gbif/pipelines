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
class OrganismJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("OrganismJoinBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> occurrenceDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("organismID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> organismDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("organismID", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add("associatedOrganisms", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- tests ----

  @Test
  void absentOrganismTable_returnsOriginalDf() {
    Dataset<Row> occ =
        occurrenceDf(List.of(RowFactory.create("occ-1", "evt-1", "org-1", "Parus major")));

    Dataset<Row> result = OrganismJoinBuilder.enrichOccurrences(TestTableLoader.of(), occ);

    assertEquals(occ.columns().length, result.columns().length);
    assertEquals(1L, result.count());
  }

  @Test
  void occurrenceWithoutOrganismIdColumn_returnsOriginalDf() {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    Dataset<Row> occ =
        spark.createDataFrame(List.of(RowFactory.create("occ-1", "Parus major")), schema);
    Dataset<Row> orgDf = organismDf(List.of(RowFactory.create("org-1", "Blue tit", null)));

    Dataset<Row> result =
        OrganismJoinBuilder.enrichOccurrences(
            TestTableLoader.of(OrganismJoinBuilder.TABLE_ORGANISM, orgDf), occ);

    assertEquals(2, result.columns().length, "Should be original columns only");
    assertEquals(1L, result.count());
  }

  @Test
  void joinAddsOrganismColumnsAbsentFromOccurrence() {
    Dataset<Row> occ =
        occurrenceDf(List.of(RowFactory.create("occ-1", "evt-1", "org-1", "Parus major")));
    Dataset<Row> orgDf =
        organismDf(List.of(RowFactory.create("org-1", "Blue tit", "sibling of:org-2")));

    Dataset<Row> result =
        OrganismJoinBuilder.enrichOccurrences(
            TestTableLoader.of(OrganismJoinBuilder.TABLE_ORGANISM, orgDf), occ);

    List<String> cols = Arrays.asList(result.columns());
    assertTrue(cols.contains("associatedOrganisms"), "associatedOrganisms should be added");
    assertTrue(cols.contains("occurrenceID"), "occurrenceID should be preserved");

    Row row = result.filter(result.col("occurrenceID").equalTo("occ-1")).first();
    assertEquals("Blue tit", row.getAs("organismName"));
    assertEquals("sibling of:org-2", row.getAs("associatedOrganisms"));
  }

  @Test
  void manyOccurrencesToOneOrganism_allRowsGetOrganismFields() {
    Dataset<Row> occ =
        occurrenceDf(
            List.of(
                RowFactory.create("occ-1", "evt-1", "org-1", "Parus major"),
                RowFactory.create("occ-2", "evt-2", "org-1", "Parus major"),
                RowFactory.create("occ-3", "evt-3", "org-1", "Parus major")));
    Dataset<Row> orgDf = organismDf(List.of(RowFactory.create("org-1", "Blue tit", null)));

    Dataset<Row> result =
        OrganismJoinBuilder.enrichOccurrences(
            TestTableLoader.of(OrganismJoinBuilder.TABLE_ORGANISM, orgDf), occ);

    assertEquals(3L, result.count(), "All 3 occurrences should survive the join");
    assertEquals(
        1L,
        result.select("organismName").distinct().count(),
        "All rows should carry the same organism name");
  }

  @Test
  void occurrenceWithNoMatchingOrganism_survivesWithNullOrganismFields() {
    Dataset<Row> occ =
        occurrenceDf(List.of(RowFactory.create("occ-1", "evt-1", "org-UNKNOWN", "Parus major")));
    Dataset<Row> orgDf = organismDf(List.of(RowFactory.create("org-1", "Blue tit", null)));

    Dataset<Row> result =
        OrganismJoinBuilder.enrichOccurrences(
            TestTableLoader.of(OrganismJoinBuilder.TABLE_ORGANISM, orgDf), occ);

    assertEquals(1L, result.count(), "Row should survive left join even with no organism match");
    Row row = result.first();
    assertTrue(row.isNullAt(row.fieldIndex("associatedOrganisms")));
  }

  @Test
  void joinOrganism_doesNotDuplicateColumnsAlreadyOnOccurrence() {
    Dataset<Row> occ =
        occurrenceDf(List.of(RowFactory.create("occ-1", "evt-1", "org-1", "Parus major")));
    Dataset<Row> orgDf = organismDf(List.of(RowFactory.create("org-1", "Blue tit", null)));

    Dataset<Row> result = OrganismJoinBuilder.joinOrganism(occ, orgDf);

    long distinctColCount = Arrays.stream(result.columns()).distinct().count();
    assertEquals(
        result.columns().length, distinctColCount, "Result must have no duplicate column names");
  }
}
