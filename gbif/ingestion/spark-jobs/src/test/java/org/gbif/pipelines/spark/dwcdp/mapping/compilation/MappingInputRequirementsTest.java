package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingInputRequirements;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventDwcaMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.config.OccurrenceDwcaMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MappingInputRequirementsTest {
  private static DwcDpMappingEngine engine;

  @BeforeAll
  static void setup() {
    SchemaGraph graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
  }

  @Test
  void eventPlanIncludesCoreIdentityParentJoinAndMappedTargetColumns() {
    MappingInputRequirements requirements =
        engine.inputRequirements(EventDwcaMapping.current(engine.schemaGraph()));

    MappingInputRequirements.ResourceRequirement event = requirements.resource("event");
    assertTrue(event.columns().contains("event_pk"));
    assertTrue(event.columns().contains("eventID"));
    assertTrue(event.columns().contains("parentEvent_fk"));
    assertTrue(event.columns().contains("eventDate"));

    MappingInputRequirements.ResourceRequirement protocol = requirements.resource("protocol");
    assertTrue(protocol.columns().contains("protocol_pk"));
    assertTrue(protocol.columns().contains("protocolDescription"));
  }

  @Test
  void occurrencePlanIncludesAttachmentAndExactlyOneJoinKeys() {
    MappingInputRequirements requirements =
        engine.inputRequirements(OccurrenceDwcaMapping.current(engine.schemaGraph()));

    MappingInputRequirements.ResourceRequirement occurrence = requirements.resource("occurrence");
    assertTrue(occurrence.columns().contains("occurrence_pk"));
    assertTrue(occurrence.columns().contains("occurrenceID"));

    MappingInputRequirements.ResourceRequirement material = requirements.resource("material");
    assertTrue(material.columns().contains("evidenceForOccurrenceID"));

    MappingInputRequirements.ResourceRequirement identification =
        requirements.resource("identification");
    assertTrue(identification.columns().contains("occurrence_fk"));
  }

  @Test
  void resourcesOutsideCanonicalPlanAreNotRequired() {
    MappingInputRequirements requirements =
        engine.inputRequirements(EventDwcaMapping.current(engine.schemaGraph()));

    assertFalse(requirements.usesResource("chronometric-age"));
  }
  @Test
  void declarativeFiltersContributeOnlyTheirReferencedColumns() {
    MappingInputRequirements requirements =
        engine.inputRequirements(OccurrenceDwcaMapping.current(engine.schemaGraph()));

    MappingInputRequirements.ResourceRequirement identification =
        requirements.resource("identification");
    assertFalse(identification.allColumns());
    assertTrue(identification.columns().contains("isAcceptedIdentification"));
  }

  @Test
  void declarativeProtocolFilterDoesNotDisableProjection() {
    MappingInputRequirements requirements =
        engine.inputRequirements(EventDwcaMapping.current(engine.schemaGraph()));

    MappingInputRequirements.ResourceRequirement protocol = requirements.resource("protocol");
    assertFalse(protocol.allColumns());
    assertTrue(protocol.columns().contains("protocolType"));
  }

}
