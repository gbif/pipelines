package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledSourceField;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledTargetProducer;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkTargetExpressionTest {

  private SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("SparkTargetExpressionTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void delimitedAggregationUsesAllSourcesAndIsDeterministic() {
    Dataset<Row> rows =
        spark.createDataFrame(
            List.of(RowFactory.create("b", "a"), RowFactory.create("c", null)),
            new StructType().add("left", DataTypes.StringType).add("right", DataTypes.StringType));

    CompiledTargetProducer target = delimitedTarget("left", "right");
    Column value =
        SparkTargetExpression.aggregate(
            target,
            List.of(rows.col("left"), rows.col("right")),
            Optional.empty(),
            Optional.empty());

    String result = rows.agg(value.as("value")).first().getAs("value");
    assertEquals("a|b|c", result);
  }

  @Test
  void emptyDelimitedAggregationProducesNull() {
    Dataset<Row> rows =
        spark.createDataFrame(
            List.of(RowFactory.create(null, null)),
            new StructType().add("left", DataTypes.StringType).add("right", DataTypes.StringType));

    CompiledTargetProducer target = delimitedTarget("left", "right");
    Column value =
        SparkTargetExpression.aggregate(
            target,
            List.of(rows.col("left"), rows.col("right")),
            Optional.empty(),
            Optional.empty());

    String result = rows.agg(value.as("value")).first().getAs("value");
    assertNull(result);
  }

  @Test
  void firstOrUrnFallbackUsesNaturalIdAndFallsBackForNullOrBlank() {
    Dataset<Row> rows =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create("OCC-1", "PK-1"),
                RowFactory.create(null, "PK-2"),
                RowFactory.create("   ", "PK-3")),
            new StructType()
                .add("naturalId", DataTypes.StringType)
                .add("primaryKey", DataTypes.StringType));

    CompiledTargetProducer target = firstOrUrnFallbackTarget();
    Column value =
        SparkTargetExpression.row(target, List.of(rows.col("naturalId"), rows.col("primaryKey")));

    List<String> values =
        rows.select(value.as("value")).collectAsList().stream()
            .map(row -> (String) row.getAs("value"))
            .toList();

    assertEquals(
        List.of("OCC-1", "urn:gbif:dwcdp:occurrence:PK-2", "urn:gbif:dwcdp:occurrence:PK-3"),
        values);
  }

  @Test
  void firstOrUrnFallbackAggregationUsesNaturalIdAndFallsBackForNullOrBlank() {
    Dataset<Row> rows =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create("a", "OCC-1", "PK-1"),
                RowFactory.create("b", null, "PK-2"),
                RowFactory.create("c", "   ", "PK-3")),
            new StructType()
                .add("group", DataTypes.StringType)
                .add("naturalId", DataTypes.StringType)
                .add("primaryKey", DataTypes.StringType));

    CompiledTargetProducer target = firstOrUrnFallbackTarget();
    Column value =
        SparkTargetExpression.aggregate(
            target,
            List.of(rows.col("naturalId"), rows.col("primaryKey")),
            Optional.empty(),
            Optional.empty());

    List<String> values =
        rows.groupBy("group").agg(value.as("value")).orderBy("group").collectAsList().stream()
            .map(row -> (String) row.getAs("value"))
            .toList();

    assertEquals(
        List.of("OCC-1", "urn:gbif:dwcdp:occurrence:PK-2", "urn:gbif:dwcdp:occurrence:PK-3"),
        values);
  }

  private static CompiledTargetProducer firstOrUrnFallbackTarget() {
    return new CompiledTargetProducer(
        "technicalId",
        "test",
        TargetFieldMapping.SourceMode.ONE_OF,
        ValueAggregation.firstOrUrnFallback("urn:gbif:dwcdp:occurrence:"),
        List.of(
            new CompiledSourceField(SchemaPath.root("source").field("naturalId")),
            new CompiledSourceField(SchemaPath.root("source").field("primaryKey"))),
        TargetFieldMapping.Origin.EXPLICIT,
        Optional.empty(),
        Optional.empty());
  }

  private static CompiledTargetProducer delimitedTarget(String... sourceColumns) {
    List<CompiledSourceField> sources =
        Arrays.stream(sourceColumns)
            .map(column -> new CompiledSourceField(SchemaPath.root("source").field(column)))
            .toList();
    return new CompiledTargetProducer(
        "target",
        "test",
        TargetFieldMapping.SourceMode.ALL_OF,
        ValueAggregation.pipeDelimitedDistinct(),
        sources,
        TargetFieldMapping.Origin.EXPLICIT,
        Optional.empty(),
        Optional.empty());
  }
}
