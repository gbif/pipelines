package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledTargetMerge;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class OccurrenceFallbackMappingTest {

  private static SchemaGraph graph;
  private static MappingCompiler compiler;

  @BeforeAll
  static void setup() {
    graph = new DwcDpSchemaLoader().current();
    compiler = new MappingCompiler(graph);
  }

  @Test
  void eventOccurrenceKeepsKnownIdentificationFallbackProducers() {
    CompiledMapping compiled = compiler.compile(EventDwcaMapping.current(graph));
    CompiledExtension occurrence =
        compiled.extensions().stream()
            .filter(extension -> extension.rowType().equals(OccurrenceMapping.ROW_TYPE_OCCURRENCE))
            .findFirst()
            .orElseThrow();

    CompiledTargetMerge dateIdentified = merge(occurrence.targetMerges(), "dateIdentified");
    assertEquals(
        List.of("occurrence-direct", "occurrence-accepted-identification", "occurrence-material"),
        dateIdentified.producers().stream().map(producer -> producer.owner()).toList());

    CompiledTargetMerge verbatimIdentification =
        merge(occurrence.targetMerges(), "verbatimIdentification");
    assertTrue(
        verbatimIdentification.producers().stream()
            .map(producer -> producer.owner())
            .toList()
            .containsAll(
                List.of(
                    "occurrence-direct",
                    "occurrence-accepted-identification",
                    "occurrence-material")));

    CompiledTargetMerge vernacularName = merge(occurrence.targetMerges(), "vernacularName");
    assertTrue(
        vernacularName.producers().stream()
            .map(producer -> producer.owner())
            .toList()
            .containsAll(
                List.of(
                    "occurrence-direct",
                    "occurrence-accepted-identification",
                    "occurrence-material",
                    "occurrence-accepted-identification-taxon")));
  }

  @Test
  void occurrenceCoreKeepsAgentRoleAndTaxonFallbacks() {
    CompiledMapping compiled = compiler.compile(OccurrenceDwcaMapping.current(graph));

    CompiledTargetMerge identifiedBy = merge(compiled.coreTargetMerges(), "identifiedBy");
    assertTrue(
        identifiedBy.producers().stream()
            .map(producer -> producer.owner())
            .toList()
            .contains("occurrence-core-accepted-identification-agent-roles"));

    CompiledTargetMerge identifiedById = merge(compiled.coreTargetMerges(), "identifiedByID");
    assertTrue(
        identifiedById.producers().stream()
            .map(producer -> producer.owner())
            .toList()
            .contains("occurrence-core-accepted-identification-agent-roles"));

    CompiledTargetMerge scientificName = merge(compiled.coreTargetMerges(), "scientificName");
    assertTrue(
        scientificName.producers().stream()
            .map(producer -> producer.owner())
            .toList()
            .contains("occurrence-core-accepted-identification-taxon"));
  }

  private static CompiledTargetMerge merge(List<CompiledTargetMerge> merges, String localName) {
    String target = TargetTerms.resolve(localName);
    return merges.stream()
        .filter(candidate -> candidate.targetTerm().equals(target))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing target merge for " + target));
  }
}
