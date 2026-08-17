package org.gbif.pipelines.spark.dwcdp.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
class SparkMappingPathExecutorTest {
  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("SparkMappingPathExecutorTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void filterAndFanOutUseRealSchemaRelationsAndEmitMetrics() {
    Mapping mapping =
        MappingBuilder.mapping("survey-collectors", "survey")
            .join("survey-agent-role")
            .filter(cols -> cols.col("agentRole").equalTo("collector"))
            .fanOut()
            .join("agent")
            .exactlyOne()
            .build();

    MappingExecutionResult result =
        executor().execute(
            TestTableLoader.of(
                "survey", survey(),
                "survey-agent-role", roles(),
                "agent", agents()),
            mapping);

    assertTrue(result.completePath());
    assertEquals(2, result.pathResult().dataset().count(), "observer must be filtered before fan-out");

    SchemaRelation surveyRoles = graph.resolve("survey", "survey-agent-role", "survey_fk");
    SchemaRelation roleAgent = graph.resolve("survey-agent-role", "agent", "agent_fk");
    SchemaPath rolePath = SchemaPath.root("survey").append(surveyRoles);
    SchemaPath agentPath = rolePath.append(roleAgent);

    List<String> names =
        result.pathResult().dataset()
            .select(result.pathResult().column(agentPath.field("preferredAgentName")))
            .collectAsList()
            .stream()
            .map(r -> r.getString(0))
            .sorted()
            .toList();
    assertEquals(List.of("Alice", "Bob"), names);

    RelationExecutionMetrics roleMetrics = result.metrics().get(0);
    assertEquals(3, roleMetrics.targetRowsBeforeFilter());
    assertEquals(2, roleMetrics.targetRowsAfterFilter());
    assertEquals(1, roleMetrics.inputRows());
    assertEquals(2, roleMetrics.outputRows());

    RelationExecutionMetrics agentMetrics = result.metrics().get(1);
    assertEquals(2, agentMetrics.inputRows());
    assertEquals(2, agentMetrics.matchedParentRows());
    assertEquals(0, agentMetrics.unmatchedParentRows());
  }

  @Test
  void exactlyOneKeepsParentButNullsTargetWhenMultipleRowsMatch() {
    Mapping mapping =
        MappingBuilder.mapping("exactly-one-role", "survey")
            .join("survey-agent-role")
            .exactlyOne()
            .build();

    MappingExecutionResult result =
        executor().execute(
            TestTableLoader.of("survey", survey(), "survey-agent-role", roles()), mapping);

    assertEquals(1, result.pathResult().dataset().count());
    assertEquals(1, result.metrics().get(0).multipleMatchParentRows());

    SchemaRelation relation = graph.resolve("survey", "survey-agent-role", "survey_fk");
    SchemaPath rolePath = SchemaPath.root("survey").append(relation);
    Row row = result.pathResult().dataset().first();
    String agentRole =
        row.getAs(result.pathResult().columnName(rolePath.field("agentRole")));
    assertNull(agentRole);
  }


  @Test
  void exactlyOneIgnoresDuplicateIdenticalTargetRows() {
    Mapping mapping =
        MappingBuilder.mapping("exactly-one-duplicate-role", "survey")
            .join("survey-agent-role")
            .exactlyOne()
            .build();

    Dataset<Row> duplicateRoles =
        spark.createDataFrame(
            List.of(
                RowFactory.create("S1", "A1", "collector", 1),
                RowFactory.create("S1", "A1", "collector", 1)),
            new StructType()
                .add("survey_fk", DataTypes.StringType)
                .add("agent_fk", DataTypes.StringType)
                .add("agentRole", DataTypes.StringType)
                .add("agentRoleOrder", DataTypes.IntegerType));

    MappingExecutionResult result =
        executor().execute(
            TestTableLoader.of("survey", survey(), "survey-agent-role", duplicateRoles), mapping);

    SchemaRelation relation = graph.resolve("survey", "survey-agent-role", "survey_fk");
    SchemaPath rolePath = SchemaPath.root("survey").append(relation);
    Row row = result.pathResult().dataset().first();
    String agentRole = row.getAs(result.pathResult().columnName(rolePath.field("agentRole")));

    assertEquals("collector", agentRole);
    assertEquals(0, result.metrics().get(0).multipleMatchParentRows());
  }

  @Test
  void selectorPicksOneTargetRowDeterministically() {
    Mapping mapping =
        MappingBuilder.mapping("first-role", "survey")
            .join("survey-agent-role")
            .select("agentRoleOrder")
            .build();

    MappingExecutionResult result =
        executor().execute(
            TestTableLoader.of("survey", survey(), "survey-agent-role", roles()), mapping);

    SchemaRelation relation = graph.resolve("survey", "survey-agent-role", "survey_fk");
    SchemaPath rolePath = SchemaPath.root("survey").append(relation);
    Row row = result.pathResult().dataset().first();
    Integer agentRoleOrder =
        row.getAs(result.pathResult().columnName(rolePath.field("agentRoleOrder")));
    assertEquals(1, agentRoleOrder);
  }

  @Test
  void optionalMissingResourcePreservesSourceAndMaterializesNullTargetPath() {
    Mapping mapping =
        MappingBuilder.mapping("optional-agents", "survey")
            .join("survey-agent-role")
            .fanOut()
            .build();

    MappingExecutionResult result =
        executor().execute(TestTableLoader.of("survey", survey()), mapping);

    assertTrue(result.completePath());
    assertEquals(1, result.pathResult().dataset().count());
    assertTrue(result.metrics().get(0).skipped());

    SchemaRelation relation = graph.resolve("survey", "survey-agent-role", "survey_fk");
    SchemaPath rolePath = SchemaPath.root("survey").append(relation);
    Row row = result.pathResult().dataset().first();
    String agentRole = row.getAs(result.pathResult().columnName(rolePath.field("agentRole")));
    assertNull(agentRole);
  }

  @Test
  void requiredMissingResourceFails() {
    Mapping mapping =
        MappingBuilder.mapping("required-agents", "survey")
            .join("survey-agent-role")
            .required()
            .fanOut()
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> executor().execute(TestTableLoader.of("survey", survey()), mapping));
    assertTrue(ex.getMessage().contains("survey-agent-role"));
  }

  @Test
  void relationCombineFailsFastUntilTargetMaterializationOwnsIt() {
    Mapping mapping =
        MappingBuilder.mapping("unsafe-combine", "survey")
            .join("survey-agent-role")
            .combine(ValueAggregation.pipeDelimitedDistinct())
            .build();

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            executor().execute(
                TestTableLoader.of("survey", survey(), "survey-agent-role", roles()), mapping));
  }

  private SparkMappingPathExecutor executor() {
    return new SparkMappingPathExecutor(graph);
  }

  private Dataset<Row> survey() {
    return spark.createDataFrame(
        List.of(RowFactory.create("S1", "E1")),
        new StructType()
            .add("survey_pk", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType));
  }

  private Dataset<Row> roles() {
    return spark.createDataFrame(
        List.of(
            RowFactory.create("S1", "A1", "collector", 1),
            RowFactory.create("S1", "A2", "collector", 2),
            RowFactory.create("S1", "A3", "observer", 3)),
        new StructType()
            .add("survey_fk", DataTypes.StringType)
            .add("agent_fk", DataTypes.StringType)
            .add("agentRole", DataTypes.StringType)
            .add("agentRoleOrder", DataTypes.IntegerType));
  }

  private Dataset<Row> agents() {
    return spark.createDataFrame(
        List.of(
            RowFactory.create("A1", "Alice"),
            RowFactory.create("A2", "Bob"),
            RowFactory.create("A3", "Carol")),
        new StructType()
            .add("agent_pk", DataTypes.StringType)
            .add("preferredAgentName", DataTypes.StringType));
  }
}
