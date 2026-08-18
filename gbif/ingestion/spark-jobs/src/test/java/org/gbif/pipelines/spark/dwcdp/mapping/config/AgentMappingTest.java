package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.ExtensionMaterializationResult;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkExtensionMaterializer;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AgentMappingTest {

  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("AgentMappingTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void explicitTextWinsAndAgentIdProvidesFallback() {
    Dataset<Row> surveys =
        spark.createDataFrame(
            List.of(
                RowFactory.create("S1", "E1", "Publisher name", "agent-1"),
                RowFactory.create("S2", "E1", null, "agent-2")),
            new StructType()
                .add("survey_pk", DataTypes.StringType)
                .add("event_fk", DataTypes.StringType)
                .add("identifiedBy", DataTypes.StringType)
                .add("identifiedByID", DataTypes.StringType));
    Dataset<Row> agents =
        spark.createDataFrame(
            List.of(
                RowFactory.create("A1", "agent-1", "Resolved one"),
                RowFactory.create("A2", "agent-2", "Resolved two")),
            new StructType()
                .add("agent_pk", DataTypes.StringType)
                .add("agentID", DataTypes.StringType)
                .add("preferredAgentName", DataTypes.StringType));

    ExtensionMaterializationResult result =
        new SparkExtensionMaterializer(graph)
            .materialize(
                TestTableLoader.of("survey", surveys, "agent", agents),
                new ExtensionMapping(
                    HumboldtMapping.ROW_TYPE_HUMBOLDT,
                    List.of(HumboldtMapping.surveyRows(), HumboldtMapping.identifiedBy(graph))));

    List<Row> rows = result.dataset().orderBy(result.parentKeyColumn()).collectAsList();
    assertEquals(
        "Publisher name",
        rows.get(0).getAs(result.columnName(EcoTerm.identifiedBy.qualifiedName())));
    assertEquals(
        "Resolved two",
        rows.get(1).getAs(result.columnName(EcoTerm.identifiedBy.qualifiedName())));
  }
}
