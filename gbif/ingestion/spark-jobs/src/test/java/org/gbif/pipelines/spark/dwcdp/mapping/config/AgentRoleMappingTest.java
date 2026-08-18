package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.ExtensionMaterializationResult;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkExtensionMaterializer;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AgentRoleMappingTest {

  private static final String ROW_TYPE = "http://example.org/Humboldt";
  private static final String TARGET = "http://example.org/collectorNames";
  private static final String SURVEY_ID_TARGET = "http://example.org/surveyKey";

  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("AgentRoleMappingTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void surveyCollectorsAreFilteredOrderedAndDeduplicatedByRenderedName() {
    ExtensionFragment fragment = surveyCollectors();

    Dataset<Row> roles =
        spark.createDataFrame(
            List.of(
                RowFactory.create("S1", "A1", "collector", 2),
                RowFactory.create("S1", "A2", "observer", 1),
                RowFactory.create("S1", "A3", "collector", 1),
                RowFactory.create("S1", "A4", "collector", 3)),
            new StructType()
                .add("survey_fk", DataTypes.StringType)
                .add("agent_fk", DataTypes.StringType)
                .add("agentRole", DataTypes.StringType)
                .add("agentRoleOrder", DataTypes.IntegerType));
    Dataset<Row> agents =
        spark.createDataFrame(
            List.of(
                RowFactory.create("A1", "Bob"),
                RowFactory.create("A2", "Ignored"),
                RowFactory.create("A3", "Zoe"),
                RowFactory.create("A4", "Zoe")),
            new StructType()
                .add("agent_pk", DataTypes.StringType)
                .add("preferredAgentName", DataTypes.StringType));

    ExtensionMaterializationResult result =
        new SparkExtensionMaterializer(graph)
            .materialize(
                TestTableLoader.of(
                    "survey", surveys(),
                    "survey-agent-role", roles,
                    "agent", agents),
                new ExtensionMapping(ROW_TYPE, List.of(surveyRows(), fragment)));

    List<Row> rows = result.dataset().orderBy(result.parentKeyColumn()).collectAsList();
    assertEquals(2, rows.size());
    assertEquals("S1", rows.get(0).getAs(result.parentKeyColumn()));
    assertEquals("Zoe|Bob", rows.get(0).getAs(result.columnName(TARGET)));
    assertEquals("S2", rows.get(1).getAs(result.parentKeyColumn()));
    String secondTarget = rows.get(1).getAs(result.columnName(TARGET));
    assertNull(secondTarget);
  }

  @Test
  void danglingAgentDoesNotContributeAName() {
    ExtensionFragment fragment = surveyCollectors();
    Dataset<Row> roles =
        spark.createDataFrame(
            List.of(RowFactory.create("S1", "missing", "collector", 1)),
            new StructType()
                .add("survey_fk", DataTypes.StringType)
                .add("agent_fk", DataTypes.StringType)
                .add("agentRole", DataTypes.StringType)
                .add("agentRoleOrder", DataTypes.IntegerType));

    ExtensionMaterializationResult result =
        new SparkExtensionMaterializer(graph)
            .materialize(
                TestTableLoader.of(
                    "survey", surveys(),
                    "survey-agent-role", roles,
                    "agent", agents()),
                new ExtensionMapping(ROW_TYPE, List.of(surveyRows(), fragment)));

    List<Row> rows = result.dataset().orderBy(result.parentKeyColumn()).collectAsList();
    assertEquals(2, rows.size());
    String target = rows.get(0).getAs(result.columnName(TARGET));
    assertNull(target);
  }

  @Test
  void compiledFragmentRetainsRoleFilterIdentityAndOrderingDependencies() {
    ExtensionFragment fragment = surveyCollectors();
    SchemaPath survey = SchemaPath.root("survey");
    SchemaPath role =
        survey.append(graph.resolve("survey", "survey-agent-role", "survey_fk", null));
    SchemaPath agent =
        role.append(graph.resolve("survey-agent-role", "agent", "agent_fk", null));

    TargetFieldMapping target = fragment.fields().get(0);
    assertEquals(List.of(agent.field("preferredAgentName")), target.sources());
    assertEquals(role.field("agent_fk"), target.contributionIdentity().orElseThrow());
    assertEquals(role.field("agentRoleOrder"), target.orderBy().orElseThrow());
    assertEquals(
        Set.of("agentRole"),
        fragment.relations().get(0).filter().requiredColumns());

    MappingPlan plan =
        org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder.mappingPlan(
                "agent-role-test", CoreType.EVENT, "event")
            .coreField(
                TargetFieldMapping.oneOf(
                    "http://rs.tdwg.org/dwc/terms/eventID",
                    ValueAggregation.firstNonNull(),
                    SchemaPath.root("event").field("eventID")))
            .extension(ROW_TYPE)
            .importFragment(fragment)
            .build();
    CompiledMapping compiled = new MappingCompiler(graph).compile(plan);
    CompiledFragment compiledFragment = compiled.extensions().get(0).fragments().get(0);

    assertEquals("survey-agent-role", compiledFragment.relations().get(0).relation().targetResource());
    assertEquals("agent", compiledFragment.relations().get(1).relation().targetResource());
    assertTrue(compiledFragment.relations().get(0).filter().requiredColumns().contains("agentRole"));
  }


  @Test
  void coreVariantUsesTheSameAgentRolePolicy() {
    var fragment =
        AgentRoleMapping.core(
            graph,
            AgentRoleMapping.Spec.orderedDistinctNames(
                "survey-collectors-core",
                "survey",
                "survey-agent-role",
                "survey_fk",
                "collector",
                TARGET));

    SchemaPath survey = SchemaPath.root("survey");
    SchemaPath role =
        survey.append(graph.resolve("survey", "survey-agent-role", "survey_fk", null));
    SchemaPath agent =
        role.append(graph.resolve("survey-agent-role", "agent", "agent_fk", null));
    TargetFieldMapping target = fragment.fields().get(0);

    assertEquals(List.of(agent.field("preferredAgentName")), target.sources());
    assertEquals(role.field("agent_fk"), target.contributionIdentity().orElseThrow());
    assertEquals(role.field("agentRoleOrder"), target.orderBy().orElseThrow());
  }

  private ExtensionFragment surveyCollectors() {
    return AgentRoleMapping.extension(
        graph,
        ROW_TYPE,
        AgentRoleMapping.Spec.orderedDistinctNames(
            "survey-collectors",
            "survey",
            "survey-agent-role",
            "survey_fk",
            "collector",
            TARGET));
  }


  private ExtensionFragment surveyRows() {
    SchemaPath survey = SchemaPath.root("survey");
    return ExtensionFragmentBuilder.extensionFragment("survey-test-rows", ROW_TYPE, "survey")
        .rowIdentity(survey.field("survey_pk"))
        .field(
            TargetFieldMapping.oneOf(
                SURVEY_ID_TARGET,
                ValueAggregation.firstNonNull(),
                survey.field("survey_pk")))
        .build();
  }

  private Dataset<Row> surveys() {
    return spark.createDataFrame(
        List.of(RowFactory.create("S1", "E1"), RowFactory.create("S2", "E1")),
        new StructType()
            .add("survey_pk", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType));
  }

  private Dataset<Row> agents() {
    return spark.createDataFrame(
        List.of(RowFactory.create("A1", "Alice")),
        new StructType()
            .add("agent_pk", DataTypes.StringType)
            .add("preferredAgentName", DataTypes.StringType));
  }
}
