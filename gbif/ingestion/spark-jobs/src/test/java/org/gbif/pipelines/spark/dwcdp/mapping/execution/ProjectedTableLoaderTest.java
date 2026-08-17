package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import org.gbif.pipelines.spark.dwcdp.mapping.execution.ProjectedTableLoader;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingInputRequirements;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingInputRequirements.ResourceRequirement;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ProjectedTableLoaderTest {
  private static SparkSession spark;

  @BeforeAll
  static void setup() {
    spark = SparkTestSession.createBuilder().appName("ProjectedTableLoaderTest").getOrCreate();
  }

  @AfterAll
  static void teardown() {
    spark.stop();
  }

  @Test
  void projectsOnlyRequiredPhysicalColumnsAndHidesUnusedResources() {
    Dataset<Row> event =
        spark.createDataFrame(
            java.util.List.of(RowFactory.create("EPK-1", "EV1", "unused")),
            new StructType()
                .add("event_pk", DataTypes.StringType)
                .add("eventID", DataTypes.StringType)
                .add("publisherExtra", DataTypes.StringType));
    Dataset<Row> agent =
        spark.createDataFrame(
            java.util.List.of(RowFactory.create("A1")),
            new StructType().add("agent_pk", DataTypes.StringType));

    TableLoader delegate =
        resource ->
            switch (resource) {
              case "event" -> Optional.of(event);
              case "agent" -> Optional.of(agent);
              default -> Optional.empty();
            };

    Map<String, ResourceRequirement> resources = new LinkedHashMap<>();
    resources.put("event", new ResourceRequirement(Set.of("event_pk", "eventID"), false));
    MappingInputRequirements requirements = new MappingInputRequirements(resources);

    TableLoader projected = ProjectedTableLoader.wrap(delegate, requirements);
    Dataset<Row> loaded = projected.load("event").orElseThrow();

    assertArrayEquals(new String[] {"event_pk", "eventID"}, loaded.columns());
    assertTrue(projected.load("agent").isEmpty());
  }

  @Test
  void allColumnsEscapeHatchPreservesFilteredResourceShape() {
    Dataset<Row> roles =
        spark.createDataFrame(
            java.util.List.of(RowFactory.create("S1", "collector", 1)),
            new StructType()
                .add("survey_fk", DataTypes.StringType)
                .add("agentRole", DataTypes.StringType)
                .add("agentRoleOrder", DataTypes.IntegerType));

    TableLoader delegate =
        resource ->
            resource.equals("survey-agent-role") ? Optional.of(roles) : Optional.empty();
    MappingInputRequirements requirements =
        new MappingInputRequirements(
            Map.of(
                "survey-agent-role",
                new ResourceRequirement(Set.of("survey_fk"), true)));

    Dataset<Row> loaded =
        ProjectedTableLoader.wrap(delegate, requirements)
            .load("survey-agent-role")
            .orElseThrow();

    assertArrayEquals(roles.columns(), loaded.columns());
  }
}
