package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlanBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkExtendedRecordExecutorTest {
  private static final String HUMBOLDT = "http://rs.gbif.org/terms/1.0/Humboldt";
  private static final String TERM_SITE_COUNT = "http://rs.tdwg.org/eco/terms/siteCount";
  private static final String TERM_TARGET = "http://rs.tdwg.org/eco/terms/targetDescription";
  private static final String TERM_COLLECTORS = "http://example.org/collectorNames";

  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder().appName("SparkExtendedRecordExecutorTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void eventCoreWithReusableHumboldtFragmentsProducesExtendedRecord() {
    SchemaPath eventPath = SchemaPath.root("event");
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

    MappingPlan plan =
        MappingPlanBuilder.mappingPlan("event-core", CoreType.EVENT, "event")
            .coreField(
                TargetFieldMapping.oneOf(
                    DwcTerm.eventID.qualifiedName(),
                    ValueAggregation.firstNonNull(),
                    eventPath.field("eventID")))
            .coreField(
                TargetFieldMapping.oneOf(
                    DwcTerm.eventDate.qualifiedName(),
                    ValueAggregation.firstNonNull(),
                    eventPath.field("eventDate")))
            .extension(HUMBOLDT)
            .importFragment(surveyTargets)
            .importFragment(collectors)
            .build();

    List<ExtendedRecord> records =
        new SparkExtendedRecordExecutor(graph)
            .execute(
                TestTableLoader.of(
                    "event", events(),
                    "survey", survey(),
                    "survey-survey-target", surveyTargetLinks(),
                    "survey-target", surveyTargets(),
                    "survey-agent-role", roles(),
                    "agent", agents()),
                plan)
            .collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord record = records.get(0);
    assertEquals("EV1", record.getId());
    assertEquals(DwcTerm.Event.qualifiedName(), record.getCoreRowType());
    assertEquals("2026-08-13", record.getCoreTerms().get(DwcTerm.eventDate.qualifiedName()));

    List<Map<String, String>> humboldt = record.getExtensions().get(HUMBOLDT);
    assertNotNull(humboldt);
    assertEquals(2, humboldt.size(), "2 targets x 2 collectors must still be 2 Humboldt rows");
    for (Map<String, String> row : humboldt) {
      assertEquals("3", row.get(TERM_SITE_COUNT));
      assertEquals("Alice|Bob", row.get(TERM_COLLECTORS));
    }
    List<String> targetDescriptions =
        humboldt.stream().map(row -> row.get(TERM_TARGET)).sorted().toList();
    assertEquals(List.of("All birds", "All mammals"), targetDescriptions);
  }

  private Dataset<Row> events() {
    return spark.createDataFrame(
        List.of(RowFactory.create("E1", "EV1", "2026-08-13")),
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("eventDate", DataTypes.StringType));
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
