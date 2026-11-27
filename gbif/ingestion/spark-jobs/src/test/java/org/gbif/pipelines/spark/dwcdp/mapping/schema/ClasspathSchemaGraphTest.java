package org.gbif.pipelines.spark.dwcdp.mapping.schema;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.junit.jupiter.api.Test;

class ClasspathSchemaGraphTest {
  private final SchemaGraph graph = new DwcDpSchemaLoader().current();

  @Test
  void releaseProfileUsesPrimaryKeyEvidenceEvenWithDraftFields() throws Exception {
    DwcDpSchemaLoader loader = new DwcDpSchemaLoader();
    SchemaGraph release =
        loader.loadBundle(
            path ->
                switch (path) {
                  case "index.json" -> """
                          {"tableSchemas":[{"url":"occurrence.json"},{"url":"material.json"}]}
                          """;
                  case "occurrence.json" -> """
                          {"name":"occurrence","primaryKey":["occurrence_pk"],
                           "fields":[{"name":"occurrence_pk"},{"name":"occurrenceID"}]}
                          """;
                  case "material.json" -> """
                          {"name":"material","primaryKey":["materialEntity_pk"],
                           "fields":[{"name":"materialEntity_pk"},{"name":"evidenceForOccurrence_fk"}],
                           "foreignKeys":[{"fields":"evidenceForOccurrence_fk","predicate":"evidence for",
                            "reference":{"resource":"occurrence","fields":"occurrence_pk"}}]}
                          """;
                  default -> throw new AssertionError(path);
                });

    DataPackage dataPackage = materialPackage("https://rs.tdwg.org/dwc-dp/1.0/dwc-dp-profile.json");
    SchemaRelation evidence =
        loader
            .forPackage(dataPackage, () -> release)
            .resolve("occurrence", "material", "evidenceForOccurrenceID");
    assertEquals("occurrence_pk", evidence.sourceColumn());
    assertEquals("evidenceForOccurrence_fk", evidence.targetColumn());
    assertFalse(evidence.weak());
  }

  @Test
  void draftProfileUsesNaturalKeyEvidence() throws Exception {
    DataPackage draft =
        materialPackage("https://dwc-prerelease.rs.tdwg.org/dwc-dp/1.0_DEV/dwc-dp-profile.json");

    SchemaRelation evidence =
        new DwcDpSchemaLoader()
            .forPackage(draft)
            .resolve("occurrence", "material", "evidenceForOccurrenceID");
    assertEquals("occurrenceID", evidence.sourceColumn());
    assertEquals("evidenceForOccurrenceID", evidence.targetColumn());
    assertTrue(evidence.weak());
  }

  private static DataPackage materialPackage(String profile) throws Exception {
    return new ObjectMapper()
        .readValue(
            """
            {"profile":"%s",
             "resources":[{"name":"material","schema":{"fields":[{"name":"evidenceForOccurrenceID"}]}}]}
            """
                .formatted(profile),
            DataPackage.class);
  }

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
