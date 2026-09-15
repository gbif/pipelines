package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledSourceField;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledTargetProducer;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CaseExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.PredicateExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetValue;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkExpressionTargetTest {

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
  void compiledExpressionTargetEvaluatesByLogicalFieldRef() {
    Dataset<Row> rows =
        spark.createDataFrame(
            List.of(RowFactory.create("preserved", "Observation", "specimen")),
            new StructType()
                .add("materialCategory", DataTypes.StringType)
                .add("eventType", DataTypes.StringType)
                .add("expected", DataTypes.StringType));

    SchemaPath source = SchemaPath.root("source");
    var material = source.field("materialCategory");
    var event = source.field("eventType");
    ValueExpression expression =
        CaseExpression.builder()
            .when(
                PredicateExpression.equals(ValueExpression.field(material), "preserved"),
                "specimen")
            .when(
                PredicateExpression.equals(ValueExpression.field(event), "Observation"),
                "observation")
            .otherwise("other");

    CompiledTargetProducer target =
        new CompiledTargetProducer(
            "target",
            "test",
            new TargetValue.Expression(expression),
            List.of(new CompiledSourceField(material), new CompiledSourceField(event)),
            TargetFieldMapping.Origin.EXPLICIT,
            Optional.empty(),
            Optional.empty());

    Row actual =
        rows.select(
                rows.col("expected"),
                SparkTargetExpression.row(target, field -> rows.col(field.column())).as("actual"))
            .first();

    String expected = actual.getAs("expected");
    Object actualValue = actual.getAs("actual");
    assertEquals(expected, actualValue);
  }
}
