package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageField;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageResource;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageSchema;
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
  void outputOccurrenceCoreDetailedMappingPlan() {
    MappingPlan plan = OccurrenceDwcaMapping.current(graph);
    String targetPlan = engine.targetPlanDetailed(plan);
    System.out.println(
        "\n========== OCCURRENCE MASTER TARGET PLAN (DETAILED) ==========\n" + targetPlan);
  }

  @Test
  void outputEventCoreDetailedMappingPlan() {
    MappingPlan plan = EventDwcaMapping.current(graph);
    String targetPlan = engine.targetPlanDetailed(plan);
    System.out.println(
        "\n========== EVENT MASTER TARGET PLAN (DETAILED) ==========\n" + targetPlan);
  }

  @Test
  void outputEventCoreMappingPlan() {
    MappingPlan plan = EventDwcaMapping.current(graph);
    String targetPlan = engine.targetPlan(plan);

    System.out.println("\n========== EVENT MASTER TARGET PLAN (COMPACT) ==========\n" + targetPlan);

    assertTrue(targetPlan.contains("Mapping: event-core:current"));
    assertTrue(targetPlan.contains("View: master schema / compact"));
    assertTrue(targetPlan.contains("CORE EVENT"));
    assertTrue(targetPlan.contains("Target: " + DwcTerm.parentEventID.qualifiedName()));
    assertTrue(targetPlan.contains("EXTENSION " + OccurrenceMapping.ROW_TYPE_OCCURRENCE));
    assertTrue(targetPlan.contains("EXTENSION " + MultimediaMapping.ROW_TYPE_MULTIMEDIA));
    assertTrue(
        targetPlan.contains("EXTENSION " + AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT));
    assertTrue(targetPlan.contains("EXTENSION " + IdentifierMapping.ROW_TYPE_IDENTIFIER));
    assertTrue(targetPlan.contains("EXTENSION " + HumboldtMapping.ROW_TYPE_HUMBOLDT));
    assertTrue(targetPlan.contains("EXTENSION " + NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA));
  }

  @Test
  void outputOccurrenceCoreMappingPlan() {
    MappingPlan plan = OccurrenceDwcaMapping.current(graph);
    String targetPlan = engine.targetPlan(plan);

    System.out.println(
        "\n========== OCCURRENCE MASTER TARGET PLAN (COMPACT) ==========\n" + targetPlan);

    assertTrue(targetPlan.contains("Mapping: occurrence-core:current"));
    assertTrue(targetPlan.contains("View: master schema / compact"));
    assertTrue(targetPlan.contains("CORE OCCURRENCE"));
    assertTrue(targetPlan.contains("Target: " + DwcTerm.occurrenceID.qualifiedName()));
    assertTrue(targetPlan.contains("EXTENSION " + MultimediaMapping.ROW_TYPE_MULTIMEDIA));
    assertTrue(
        targetPlan.contains("EXTENSION " + AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT));
    assertTrue(targetPlan.contains("EXTENSION " + IdentificationMapping.ROW_TYPE_IDENTIFICATION));
    assertTrue(targetPlan.contains("EXTENSION " + IdentifierMapping.ROW_TYPE_IDENTIFIER));
    assertTrue(targetPlan.contains("EXTENSION " + NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA));
  }

  @Test
  void outputEventCoreDetailedTargetPlan() {
    String targetPlan = engine.targetPlanDetailed(EventDwcaMapping.current(graph));

    System.out.println(
        "\n========== EVENT MASTER TARGET PLAN (DETAILED) ==========\n" + targetPlan);

    assertTrue(targetPlan.contains("View: master schema / detailed"));
    assertTrue(targetPlan.contains("Producer: event-parent [EXPLICIT]"));
    assertTrue(targetPlan.contains("[SCHEMA RELATION]"));
    assertTrue(targetPlan.contains("[EXACTLY_ONE]"));
    assertTrue(targetPlan.contains("decision: EXPLICIT_MERGE"));
    assertTrue(targetPlan.contains("inferred depth:"));
  }

  @Test
  void outputDatasetTargetPlanPrunesUnavailableBranches() {
    DataPackage dataPackage =
        dataPackage(resource("event", "event_pk", "eventID", "parentEvent_fk", "eventDate"));

    String targetPlan = engine.targetPlan(EventDwcaMapping.current(graph), dataPackage);

    System.out.println(
        "\n========== EVENT DATASET TARGET PLAN (COMPACT) ==========\n" + targetPlan);

    assertTrue(targetPlan.contains("View: dataset / compact"));
    assertTrue(targetPlan.contains("Target: " + DwcTerm.eventID.qualifiedName()));
    assertTrue(targetPlan.contains("Target: " + DwcTerm.parentEventID.qualifiedName()));
    assertFalse(targetPlan.contains("EXTENSION " + HumboldtMapping.ROW_TYPE_HUMBOLDT));
    assertFalse(targetPlan.contains("EXTENSION " + MultimediaMapping.ROW_TYPE_MULTIMEDIA));
  }

  @Test
  void outputDatasetDetailedTargetPlanKeepsTargetAsAggregateRoot() {
    DataPackage dataPackage =
        dataPackage(
            resource("event", "event_pk", "eventID", "eventProtocol_fk"),
            resource(
                "protocol", "protocol_pk", "protocolType", "protocolName", "protocolDescription"));

    String targetPlan = engine.targetPlanDetailed(EventDwcaMapping.current(graph), dataPackage);

    System.out.println(
        "\n========== EVENT DATASET TARGET PLAN (DETAILED) ==========\n" + targetPlan);

    assertTrue(targetPlan.contains("View: dataset / detailed"));
    assertTrue(targetPlan.contains("Target: " + DwcTerm.samplingProtocol.qualifiedName()));
    assertTrue(targetPlan.contains("Producer: event-direct-sampling-protocol [EXPLICIT]"));
    assertTrue(targetPlan.contains("event.eventProtocol_fk -> protocol.protocol_pk"));
    assertTrue(targetPlan.contains("values: ONE_OF / LABELED_OR_FALLBACK(': ')"));
  }

  private static DataPackage dataPackage(DataPackageResource... resources) {
    DataPackage dataPackage = new DataPackage();
    dataPackage.setResources(new ArrayList<>(List.of(resources)));
    return dataPackage;
  }

  private static DataPackageResource resource(String name, String... fields) {
    DataPackageResource resource = new DataPackageResource();
    resource.setName(name);
    DataPackageSchema schema = new DataPackageSchema();
    List<DataPackageField> dataPackageFields = new ArrayList<>();
    for (String fieldName : fields) {
      DataPackageField field = new DataPackageField();
      field.setName(fieldName);
      field.setType("string");
      dataPackageFields.add(field);
    }
    schema.setFields(dataPackageFields);
    resource.setSchema(schema);
    return resource;
  }
}
