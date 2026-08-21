package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.gbif.pipelines.spark.dwcdp.mapping.schema.InMemorySchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import org.junit.jupiter.api.Test;

class MappingPathTest {

  private final SchemaGraph graph =
      new InMemorySchemaGraph()
          .relation(
              SchemaRelation.relation(
                  "occurrence",
                  "occurrence_pk",
                  "identification",
                  "occurrence_fk",
                  null,
                  RelationCardinality.ONE_TO_MANY))
          .relation(
              SchemaRelation.relation(
                  "identification",
                  "identification_pk",
                  "identification-agent-role",
                  "identification_fk",
                  null,
                  RelationCardinality.ONE_TO_MANY))
          .relation(
              SchemaRelation.relation(
                  "identification",
                  "identifiedByID",
                  "agent",
                  "agentID",
                  null,
                  RelationCardinality.MANY_TO_ONE));

  @Test
  void keepsSchemaLineageAndExecutableRelationsTogether() {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification =
        occurrence
            .join("identification")
            .via("occurrence_fk")
            .filter(FilterExpression.eq("isAcceptedIdentification", true))
            .optional()
            .exactlyOne();

    assertEquals("occurrence", occurrence.currentResource());
    assertEquals(0, occurrence.relations().size());
    assertEquals("identification", identification.currentResource());
    assertEquals(1, identification.relations().size());
    assertEquals(1, identification.schemaPath().relations().size());
    assertEquals(identification.schemaPath(), identification.field("dateIdentified").path());
  }

  @Test
  void branchesAreImmutableAndDoNotLeakRelationsIntoSiblings() {
    MappingPath identification =
        MappingPath.root(graph, "occurrence")
            .join("identification")
            .via("occurrence_fk")
            .optional()
            .exactlyOne();

    MappingPath agent = identification.join("agent").via("identifiedByID").optional().fanOut();
    MappingPath role =
        identification
            .join("identification-agent-role")
            .via("identification_fk")
            .optional()
            .fanOut();

    assertEquals(1, identification.relations().size());
    assertEquals(2, agent.relations().size());
    assertEquals(2, role.relations().size());
    assertEquals("agent", agent.currentResource());
    assertEquals("identification-agent-role", role.currentResource());
  }
}
