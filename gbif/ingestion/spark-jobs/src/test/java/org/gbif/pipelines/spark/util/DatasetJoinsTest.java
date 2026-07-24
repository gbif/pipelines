package org.gbif.pipelines.spark.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DatasetJoinsTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("DatasetJoinsTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> leftDf(List<Row> rows) {
    StructType schema =
      new StructType()
        .add("id", DataTypes.StringType)
        .add("leftOnlyField", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> rightDf(List<Row> rows) {
    StructType schema =
      new StructType()
        .add("id", DataTypes.StringType)
        .add("rightOnlyField", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- tests ----

  @Test
  void rightAbsent_returnsLeftUnchanged() {
    Dataset<Row> left = leftDf(List.of(RowFactory.create("1", "a")));

    Dataset<Row> result = DatasetJoins.leftJoinIfPresent(left, Optional.empty(), "id");

    assertSame(left, result, "must return the exact same Dataset instance when right is absent");
  }

  @Test
  void rightPresent_joinsOnKeyAndAddsRightColumns() {
    Dataset<Row> left = leftDf(List.of(RowFactory.create("1", "a")));
    Dataset<Row> right = rightDf(List.of(RowFactory.create("1", "b")));

    Dataset<Row> result = DatasetJoins.leftJoinIfPresent(left, Optional.of(right), "id");

    Row row = result.collectAsList().get(0);
    assertEquals("a", row.getAs("leftOnlyField"));
    assertEquals("b", row.getAs("rightOnlyField"));
  }

  @Test
  void rightPresent_dropsDuplicateJoinColumnFromRightSide() {
    Dataset<Row> left = leftDf(List.of(RowFactory.create("1", "a")));
    Dataset<Row> right = rightDf(List.of(RowFactory.create("1", "b")));

    Dataset<Row> result = DatasetJoins.leftJoinIfPresent(left, Optional.of(right), "id");

    long idColumnCount = Arrays.stream(result.columns()).filter(c -> c.equals("id")).count();
    assertEquals(1, idColumnCount, "the join column must appear exactly once, not duplicated");
  }

  @Test
  void rightPresent_noMatchingKey_leftRowSurvivesWithNullRightColumns() {
    Dataset<Row> left = leftDf(List.of(RowFactory.create("1", "a")));
    Dataset<Row> right = rightDf(List.of(RowFactory.create("UNMATCHED", "b")));

    Dataset<Row> result = DatasetJoins.leftJoinIfPresent(left, Optional.of(right), "id");

    assertEquals(1L, result.count(), "left row must survive a left-outer join with no match");
    Row row = result.collectAsList().get(0);
    assertFalse(
      result.filter(result.col("rightOnlyField").isNotNull()).count() > 0,
      "unmatched right side must produce null, not drop the left row");
  }
}
