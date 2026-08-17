package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.junit.jupiter.api.Test;

class DirectFieldMappingsTest {

  private final SchemaGraph graph = new DwcDpSchemaLoader().current();

  @Test
  void surveyFieldsRemainBroadAndAreMarkedInferred() {
    ExtensionFragment fragment = HumboldtMapping.surveyFields(graph);

    Set<String> sourceColumns =
        fragment.fields().stream()
            .flatMap(field -> field.sources().stream())
            .map(source -> source.column())
            .collect(Collectors.toSet());

    assertTrue(sourceColumns.contains("protocolDescriptions"));
    assertTrue(sourceColumns.contains("siteCount"));
    assertTrue(
        fragment.fields().stream()
            .allMatch(field -> field.origin() == TargetFieldMapping.Origin.INFERRED));
  }
}
