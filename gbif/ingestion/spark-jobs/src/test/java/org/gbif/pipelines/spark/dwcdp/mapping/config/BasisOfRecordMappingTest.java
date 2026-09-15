package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkValueExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BasisOfRecordMappingTest {

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
  void classifiesBasisOfRecordWithMaterialPrecedence() {
    Dataset<Row> rows =
        spark.createDataFrame(
            Arrays.asList(
                row("preserved", "Observation", "PreservedSpecimen"),
                row("fossilized", "Sensor", "FossilSpecimen"),
                row("living", "Sensor", "LivingSpecimen"),
                row("tissue", "Sensor", "MaterialSample"),
                row("DNA extract", "Observation", "MaterialSample"),
                row(null, "NucleotideAnalysis", "MaterialSample"),
                row(null, "Sensor", "MachineObservation"),
                row(null, "Observation", "HumanObservation"),
                row(null, "Other", "Occurrence"),
                row(null, null, "Occurrence")),
            new StructType()
                .add("materialEntityCategory", DataTypes.StringType)
                .add("eventType", DataTypes.StringType)
                .add("expected", DataTypes.StringType));

    SchemaPath material = SchemaPath.root("material");
    SchemaPath event = SchemaPath.root("event");
    ValueExpression expression =
        BasisOfRecordMapping.expression(
            material.field("materialEntityCategory"), event.field("eventType"));

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

  private static Row row(String materialCategory, String eventType, String expected) {
    return RowFactory.create(materialCategory, eventType, expected);
  }
}
