package org.gbif.pipelines.spark.dwcdp.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingCompilationException;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingDecisionType;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkExtensionMaterializerTest {
  private static final String HUMBOLDT = "http://rs.gbif.org/terms/1.0/Humboldt";
  private static final String TERM_SITE_COUNT = "http://rs.tdwg.org/eco/terms/siteCount";
  private static final String TERM_TARGET = "http://rs.tdwg.org/eco/terms/targetDescription";
  private static final String TERM_COLLECTORS = "http://example.org/collectorNames";

  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("SparkExtensionMaterializerTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void sourceScopeFragmentEnrichesEachExtensionRowWithoutCartesianFanOut() {
    SchemaPath surveyPath = SchemaPath.root("survey");
    SchemaRelation surveyToLink = graph.resolve("survey", "survey-survey-target", "survey_fk");
    SchemaPath linkPath = surveyPath.append(surveyToLink);
    SchemaRelation linkToTarget =
        graph.resolve("survey-survey-target", "survey-target", "surveyTarget_fk");
    SchemaPath targetPath = linkPath.append(linkToTarget);

    ExtensionFragment surveyTargets =
        ExtensionFragmentBuilder.extensionFragment("survey-targets", HUMBOLDT, "survey")
            .join("survey-survey-target")
            .via("survey_fk")
            .fanOut()
            .join("survey-target")
            .via("surveyTarget_fk")
            .exactlyOne()
            .rowIdentity(targetPath.field("surveyTarget_pk"))
            .field(
                TargetFieldMapping.oneOf(
                    TERM_SITE_COUNT,
                    ValueAggregation.firstNonNull(),
                    surveyPath.field("siteCount")))
            .field(
                TargetFieldMapping.oneOf(
                    TERM_TARGET,
                    ValueAggregation.firstNonNull(),
                    targetPath.field("surveyTargetDescription")))
            .build();

    SchemaRelation surveyToRole = graph.resolve("survey", "survey-agent-role", "survey_fk");
    SchemaPath rolePath = surveyPath.append(surveyToRole);
    SchemaRelation roleToAgent = graph.resolve("survey-agent-role", "agent", "agent_fk");
    SchemaPath agentPath = rolePath.append(roleToAgent);

    ExtensionFragment collectors =
        ExtensionFragmentBuilder.extensionFragment("survey-collectors", HUMBOLDT, "survey")
            .join("survey-agent-role")
            .via("survey_fk")
            .filter(cols -> cols.col("agentRole").equalTo("collector"))
            .fanOut()
            .join("agent")
            .via("agent_fk")
            .exactlyOne()
            .field(
                TargetFieldMapping.allOf(
                    TERM_COLLECTORS,
                    ValueAggregation.pipeDelimitedDistinct(),
                    agentPath.field("preferredAgentName")))
            .build();

    ExtensionMapping extension = new ExtensionMapping(HUMBOLDT, List.of(surveyTargets, collectors));

    ExtensionMaterializationResult result =
        new SparkExtensionMaterializer(graph)
            .materialize(
                TestTableLoader.of(
                    "survey", survey(),
                    "survey-survey-target", surveyTargetLinks(),
                    "survey-target", surveyTargets(),
                    "survey-agent-role", roles(),
                    "agent", agents()),
                extension);

    List<Row> rows = result.dataset().orderBy(result.rowKeyColumn()).collectAsList();

    assertEquals(2, rows.size(), "2 targets x 2 collectors must remain 2 extension rows, not 4");
    String parentKey = rows.get(0).getAs(result.parentKeyColumn());
    String siteCount = rows.get(0).getAs(result.columnName(TERM_SITE_COUNT));
    String firstCollectors = rows.get(0).getAs(result.columnName(TERM_COLLECTORS));
    String secondCollectors = rows.get(1).getAs(result.columnName(TERM_COLLECTORS));
    assertEquals("S1", parentKey);
    assertEquals("3", siteCount);
    assertEquals("Alice|Bob", firstCollectors);
    assertEquals("Alice|Bob", secondCollectors);

    List<String> descriptions =
        rows.stream()
            .map(row -> (String) row.getAs(result.columnName(TERM_TARGET)))
            .sorted()
            .toList();
    assertEquals(List.of("All birds", "All mammals"), descriptions);
  }

  @Test
  void duplicateTargetErrorReportsBothProducersAndPhysicalSourceAliases() {
    SchemaPath surveyPath = SchemaPath.root("survey");

    ExtensionFragment first =
        ExtensionFragmentBuilder.extensionFragment("first-fragment", HUMBOLDT, "survey")
            .rowIdentity(SchemaPath.root("survey").field("survey_pk"))
            .field(
                TargetFieldMapping.oneOf(
                    TERM_SITE_COUNT,
                    ValueAggregation.firstNonNull(),
                    surveyPath.field("siteCount")))
            .build();

    ExtensionFragment second =
        ExtensionFragmentBuilder.extensionFragment("second-fragment", HUMBOLDT, "survey")
            .field(
                TargetFieldMapping.oneOf(
                    TERM_SITE_COUNT,
                    ValueAggregation.firstNonNull(),
                    surveyPath.field("event_fk")))
            .build();

    ExtensionMapping extension = new ExtensionMapping(HUMBOLDT, List.of(first, second));

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new SparkExtensionMaterializer(graph)
                    .materialize(TestTableLoader.of("survey", survey()), extension));

    assertTrue(error instanceof MappingCompilationException);
    MappingCompilationException mappingException = (MappingCompilationException) error;
    assertEquals(1, mappingException.problems().size());
    assertEquals(
        MappingDecisionType.AMBIGUOUS_MULTIPLE_EXPLICIT,
        mappingException.problems().get(0).type());
  }

  private Dataset<Row> survey() {
    return spark.createDataFrame(
        List.of(RowFactory.create("S1", "E1", "3")),
        new StructType()
            .add("survey_pk", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("siteCount", DataTypes.StringType));
  }

  private Dataset<Row> surveyTargetLinks() {
    return spark.createDataFrame(
        List.of(RowFactory.create("S1", "T1"), RowFactory.create("S1", "T2")),
        new StructType()
            .add("survey_fk", DataTypes.StringType)
            .add("surveyTarget_fk", DataTypes.StringType));
  }

  private Dataset<Row> surveyTargets() {
    return spark.createDataFrame(
        List.of(RowFactory.create("T1", "All birds"), RowFactory.create("T2", "All mammals")),
        new StructType()
            .add("surveyTarget_pk", DataTypes.StringType)
            .add("surveyTargetDescription", DataTypes.StringType));
  }

  private Dataset<Row> roles() {
    return spark.createDataFrame(
        List.of(
            RowFactory.create("S1", "A1", "collector", 1),
            RowFactory.create("S1", "A2", "collector", 2),
            RowFactory.create("S1", "A3", "observer", 1)),
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
