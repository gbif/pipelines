package org.gbif.pipelines.spark.dwcdp.mapping;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ClasspathSchemaGraphTest {
  private final SchemaGraph graph = new DwcDpSchemaLoader().current();

  @Test
  void loadsCurrentOfficialSchemaAndPrimaryKeys() {
    SchemaResource agent = graph.resource("agent").orElseThrow();
    assertEquals("agent_pk", agent.primaryKey().orElseThrow());
    assertEquals("agentID", agent.weakPrimaryKey().orElseThrow());
    assertTrue(agent.fields().containsKey("preferredAgentName"));
  }

  @Test
  void resolvesReverseTraversalFromSurveyToSurveyAgentRole() {
    SchemaRelation relation = graph.resolve("survey", "survey-agent-role", "survey_fk");
    assertEquals("survey_pk", relation.sourceColumn());
    assertEquals("survey_fk", relation.targetColumn());
    assertEquals(RelationCardinality.ONE_TO_MANY, relation.cardinality());
  }

  @Test
  void resolvesSurveyAgentRoleToAgentFromDeclaredFk() {
    SchemaRelation relation = graph.resolve("survey-agent-role", "agent", "agent_fk");
    assertEquals("agent_fk", relation.sourceColumn());
    assertEquals("agent_pk", relation.targetColumn());
    assertEquals(RelationCardinality.MANY_TO_ONE, relation.cardinality());
  }

  @Test
  void agentAgentRoleToAgentIsAmbiguousWithoutRelationHint() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> graph.resolve("agent-agent-role", "agent"));
    assertTrue(ex.getMessage().contains("Ambiguous"));
  }
}
