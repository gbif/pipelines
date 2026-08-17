package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkPathResult;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkSchemaPathExecutor;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import static org.junit.jupiter.api.Assertions.*;

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
class SparkSchemaPathExecutorTest {
  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("SparkSchemaPathExecutorTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void executesRealSchemaPathAndKeepsPathQualifiedFieldsDistinct() {
    Dataset<Row> survey = spark.createDataFrame(
        List.of(RowFactory.create("S1", "E1")),
        new StructType().add("survey_pk", DataTypes.StringType).add("event_fk", DataTypes.StringType));
    Dataset<Row> roles = spark.createDataFrame(
        List.of(RowFactory.create("S1", "A1", "collector", 1)),
        new StructType()
            .add("survey_fk", DataTypes.StringType)
            .add("agent_fk", DataTypes.StringType)
            .add("agentRole", DataTypes.StringType)
            .add("agentRoleOrder", DataTypes.IntegerType));
    Dataset<Row> agents = spark.createDataFrame(
        List.of(RowFactory.create("A1", "agent-public-id", "Ada Collector")),
        new StructType()
            .add("agent_pk", DataTypes.StringType)
            .add("agentID", DataTypes.StringType)
            .add("preferredAgentName", DataTypes.StringType));

    SchemaRelation surveyRoles = graph.resolve("survey", "survey-agent-role", "survey_fk");
    SchemaRelation roleAgent = graph.resolve("survey-agent-role", "agent", "agent_fk");
    SchemaPath path = SchemaPath.root("survey").append(surveyRoles).append(roleAgent);

    SparkPathResult result = new SparkSchemaPathExecutor(graph).execute(
        TestTableLoader.of(
            "survey", survey,
            "survey-agent-role", roles,
            "agent", agents),
        path);

    Row row = result.dataset().first();
    FieldRef surveyPk = SchemaPath.root("survey").field("survey_pk");
    SchemaPath rolePath = SchemaPath.root("survey").append(surveyRoles);
    SchemaPath agentPath = rolePath.append(roleAgent);

    String surveyId = row.getAs(result.columnName(surveyPk));
    String agentRole = row.getAs(result.columnName(rolePath.field("agentRole")));
    String preferredAgentName =
        row.getAs(result.columnName(agentPath.field("preferredAgentName")));
    assertEquals("S1", surveyId);
    assertEquals("collector", agentRole);
    assertEquals("Ada Collector", preferredAgentName);
    assertNotEquals(
        result.columnName(rolePath.field("agent_fk")),
        result.columnName(agentPath.field("agent_pk")));
  }

  @Test
  void missingPathResourceFailsWithTheResourceName() {
    Dataset<Row> survey = spark.createDataFrame(
        List.of(RowFactory.create("S1")),
        new StructType().add("survey_pk", DataTypes.StringType));
    SchemaRelation relation = graph.resolve("survey", "survey-agent-role", "survey_fk");
    SchemaPath path = SchemaPath.root("survey").append(relation);

    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> new SparkSchemaPathExecutor(graph).execute(TestTableLoader.of("survey", survey), path));
    assertTrue(ex.getMessage().contains("survey-agent-role"));
  }
}
