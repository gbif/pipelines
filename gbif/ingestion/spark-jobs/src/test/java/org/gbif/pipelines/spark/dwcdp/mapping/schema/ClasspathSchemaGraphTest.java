package org.gbif.pipelines.spark.dwcdp.mapping.schema;

import static org.junit.jupiter.api.Assertions.*;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationCardinality;
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
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> graph.resolve("agent-agent-role", "agent"));
    assertTrue(ex.getMessage().contains("Ambiguous"));
  }

  @Test
  void selfReferenceViaColumnPrefersForwardTraversal() {
    SchemaRelation relation = graph.resolve("event", "event", "parentEvent_fk");
    assertEquals("parentEvent_fk", relation.sourceColumn());
    assertEquals("event_pk", relation.targetColumn());
    assertEquals(RelationCardinality.MANY_TO_ONE, relation.cardinality());
  }

  @Test
  void resolvesDeclaredWeakOccurrenceOrganismRelation() {
    SchemaRelation relation = graph.resolve("occurrence", "organism", "organismID");
    assertEquals("organismID", relation.sourceColumn());
    assertEquals("organismID", relation.targetColumn());
    assertTrue(relation.weak());
  }

  @Test
  void strongRelationWinsWhenStrongAndWeakCandidatesOtherwiseMatch() {
    SchemaGraph candidateGraph =
        new InMemorySchemaGraph()
            .relation(
                SchemaRelation.relation(
                    "source",
                    "reference",
                    "target",
                    "target_pk",
                    null,
                    RelationCardinality.MANY_TO_ONE,
                    true))
            .relation(
                SchemaRelation.relation(
                    "source",
                    "reference",
                    "target",
                    "target_pk",
                    null,
                    RelationCardinality.MANY_TO_ONE,
                    false));

    SchemaRelation relation = candidateGraph.resolve("source", "target", "reference");
    assertFalse(relation.weak());
  }
}
