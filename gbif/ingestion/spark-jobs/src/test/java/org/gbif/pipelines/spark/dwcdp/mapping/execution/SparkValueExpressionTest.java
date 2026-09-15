package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CaseExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.PredicateExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkValueExpressionTest {

  private SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName(getClass().getSimpleName()).getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void caseExpressionUsesOrderedPrecedenceAndDefault() {
    Dataset<Row> rows =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create("priority", "yes", "first"),
                RowFactory.create("other", "no", "second"),
                RowFactory.create("none", "no", "fallback")),
            new StructType()
                .add("kind", DataTypes.StringType)
                .add("enabled", DataTypes.StringType)
                .add("expected", DataTypes.StringType));

    SchemaPath source = SchemaPath.root("source");
    ValueExpression kind = ValueExpression.field(source.field("kind"));
    ValueExpression enabled = ValueExpression.field(source.field("enabled"));
    ValueExpression expression =
        CaseExpression.builder()
            .when(PredicateExpression.equals(enabled, "yes"), "first")
            .whenIn(kind, "second", "priority", "other")
            .otherwise("fallback");

    assertExpected(rows, expression);
  }

  @Test
  void caseExpressionSupportsWhenAllIn() {
    Dataset<Row> rows =
        spark.createDataFrame(
            List.of(
                RowFactory.create("a", "x", "both"),
                RowFactory.create("a", "z", "fallback"),
                RowFactory.create("c", "x", "fallback")),
            new StructType()
                .add("left", DataTypes.StringType)
                .add("right", DataTypes.StringType)
                .add("expected", DataTypes.StringType));

    SchemaPath source = SchemaPath.root("source");
    ValueExpression left = ValueExpression.field(source.field("left"));
    ValueExpression right = ValueExpression.field(source.field("right"));
    ValueExpression expression =
        CaseExpression.builder()
            .whenAllIn(
                "both",
                PredicateExpression.in(left, "a", "b"),
                PredicateExpression.in(right, "x", "y"))
            .otherwise("fallback");

    assertExpected(rows, expression);
  }

  @Test
  void predicateAnyProvidesGenericOrComposition() {
    Dataset<Row> rows =
        spark.createDataFrame(
            List.of(
                RowFactory.create("a", "z", "match"),
                RowFactory.create("c", "x", "match"),
                RowFactory.create("c", "z", "fallback")),
            new StructType()
                .add("left", DataTypes.StringType)
                .add("right", DataTypes.StringType)
                .add("expected", DataTypes.StringType));

    SchemaPath source = SchemaPath.root("source");
    ValueExpression left = ValueExpression.field(source.field("left"));
    ValueExpression right = ValueExpression.field(source.field("right"));
    ValueExpression expression =
        CaseExpression.builder()
            .when(
                PredicateExpression.any(
                    PredicateExpression.equals(left, "a"), PredicateExpression.equals(right, "x")),
                "match")
            .otherwise("fallback");

    assertExpected(rows, expression);
  }

  private void assertExpected(Dataset<Row> rows, ValueExpression expression) {
    Dataset<Row> actual =
        rows.select(
            rows.col("expected"),
            SparkValueExpression.build(expression, field -> rows.col(field.column())).as("actual"));

    actual
        .collectAsList()
        .forEach(
            row -> {
              String expected = row.getAs("expected");
              Object actualValue = row.getAs("actual");
              assertEquals(expected, actualValue);
            });
  }
}
