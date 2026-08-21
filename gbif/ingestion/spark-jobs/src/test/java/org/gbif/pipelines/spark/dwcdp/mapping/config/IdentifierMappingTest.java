package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class IdentifierMappingTest {

  private static SchemaGraph graph;

  @BeforeAll
  static void setup() {
    graph = new DwcDpSchemaLoader().current();
  }

  @Test
  void surveyIdentifiersArePromotedThroughSurveyToEvent() {
    ExtensionFragment fragment = IdentifierMapping.surveyIdentifiersForEvent(graph);

    assertEquals("event", fragment.sourceResource());
    assertEquals("event_pk", fragment.scopeKeyColumn().orElseThrow());
    assertEquals(2, fragment.relations().size());
    assertEquals("survey", fragment.relations().get(0).targetResource());
    assertEquals("survey-identifier", fragment.relations().get(1).targetResource());
    assertTrue(
        fragment.fields().stream()
            .anyMatch(
                field ->
                    field.sources().stream()
                        .anyMatch(source -> source.column().equals("identifier"))));
  }
}
