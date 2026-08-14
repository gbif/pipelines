package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MappingPlanOutputTest {

  private static SchemaGraph graph;
  private static DwcDpMappingEngine engine;

  @BeforeAll
  static void setup() {
    graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
  }

  @Test
  void outputEventCoreMappingPlan() {
    MappingPlan plan = EventDwcaMapping.current(graph);
    String trace = engine.trace(plan);

    System.out.println("\n========== EVENT CORE MAPPING PLAN ==========\n" + trace);

    assertTrue(trace.contains("Mapping: event-core:current"));
    assertTrue(trace.contains("Core: EVENT <- event"));
    assertTrue(trace.contains("Extension: " + OccurrenceMapping.ROW_TYPE_OCCURRENCE));
    assertTrue(trace.contains("Extension: " + MultimediaMapping.ROW_TYPE_MULTIMEDIA));
    assertTrue(trace.contains("Extension: " + AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT));
    assertTrue(trace.contains("Extension: " + IdentifierMapping.ROW_TYPE_IDENTIFIER));
    assertTrue(trace.contains("Extension: " + HumboldtMapping.ROW_TYPE_HUMBOLDT));
  }

  @Test
  void outputOccurrenceCoreMappingPlan() {
    MappingPlan plan = OccurrenceDwcaMapping.current(graph);
    String trace = engine.trace(plan);

    System.out.println("\n========== OCCURRENCE CORE MAPPING PLAN ==========\n" + trace);

    assertTrue(trace.contains("Mapping: occurrence-core:current"));
    assertTrue(trace.contains("Core: OCCURRENCE <- occurrence"));
    assertTrue(trace.contains("Extension: " + MultimediaMapping.ROW_TYPE_MULTIMEDIA));
    assertTrue(trace.contains("Extension: " + AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT));
    assertTrue(trace.contains("Extension: " + IdentificationMapping.ROW_TYPE_IDENTIFICATION));
    assertTrue(trace.contains("Extension: " + IdentifierMapping.ROW_TYPE_IDENTIFIER));
  }
}
