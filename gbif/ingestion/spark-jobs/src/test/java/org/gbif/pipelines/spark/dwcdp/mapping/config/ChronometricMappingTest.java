package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.dwc.terms.ChronoTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.junit.jupiter.api.Test;

class ChronometricMappingTest {

  private final SchemaGraph graph = new DwcDpSchemaLoader().current();

  @Test
  void baseRowsAreScopedToEventAndUseChronometricIdentity() {
    ExtensionFragment fragment = ChronometricMapping.eventAges(graph);

    assertEquals("chronometric-age", fragment.sourceResource());
    assertEquals("event_fk", fragment.scopeKeyColumn().orElseThrow());
    assertEquals("chronometricAge_pk", fragment.rowIdentity().orElseThrow().column());
    assertTrue(
        fragment.fields().stream()
            .anyMatch(field -> field.targetTerm().equals(ChronoTerm.chronometricAgeID.qualifiedName())));
  }

  @Test
  void protocolEnrichmentsMatchTheExistingChronometricRow() {
    ExtensionFragment protocol = ChronometricMapping.ageProtocol(graph);
    ExtensionFragment conversion = ChronometricMapping.conversionProtocol(graph);

    assertEquals("chronometricAge_pk", protocol.rowMatch().orElseThrow().column());
    assertEquals("protocol", protocol.relations().get(0).targetResource());
    assertTrue(
        protocol.fields().stream()
            .anyMatch(field -> field.targetTerm().equals(ChronoTerm.chronometricAgeProtocol.qualifiedName())));

    assertEquals("chronometricAge_pk", conversion.rowMatch().orElseThrow().column());
    assertEquals("protocol", conversion.relations().get(0).targetResource());
    assertTrue(
        conversion.fields().stream()
            .anyMatch(
                field ->
                    field.targetTerm()
                        .equals(ChronoTerm.chronometricAgeConversionProtocol.qualifiedName())));
  }

  @Test
  void determinerUsesSharedAgentResolutionAgainstTheSameRow() {
    ExtensionFragment fragment = ChronometricMapping.determinedBy(graph);

    assertEquals("event_fk", fragment.scopeKeyColumn().orElseThrow());
    assertEquals("chronometricAge_pk", fragment.rowMatch().orElseThrow().column());
    assertEquals("agent", fragment.relations().get(0).targetResource());
    assertTrue(
        fragment.fields().stream()
            .anyMatch(
                field ->
                    field.targetTerm().equals(ChronoTerm.chronometricAgeDeterminedBy.qualifiedName())));
  }

  @Test
  void canonicalEventPlanContainsCompleteFirstChronometricSlice() {
    MappingPlan plan = EventDwcaMapping.current(graph);
    ExtensionMapping chronometric =
        plan.extensions().stream()
            .filter(extension -> extension.rowType().equals(ChronometricMapping.ROW_TYPE_CHRONOMETRIC_AGE))
            .findFirst()
            .orElseThrow();

    assertEquals(
        Set.of(
            "event-chronometric-age",
            "event-chronometric-age-protocol",
            "event-chronometric-age-conversion-protocol",
            "event-chronometric-age-determined-by"),
        chronometric.fragments().stream().map(ExtensionFragment::name).collect(Collectors.toSet()));
  }
  @Test
  void occurrenceCorePromotesEventChronometricRowsWithSameIdentity() {
    ExtensionFragment fragment = ChronometricMapping.agesForOccurrence(graph);

    assertEquals("occurrence", fragment.sourceResource());
    assertEquals("occurrence_pk", fragment.scopeKeyColumn().orElseThrow());
    assertEquals("chronometricAge_pk", fragment.rowIdentity().orElseThrow().column());
    assertEquals(
        List.of("event", "chronometric-age"),
        fragment.relations().stream().map(relation -> relation.targetResource()).collect(Collectors.toList()));
  }

  @Test
  void chronometricMediaUsesExistingMultimediaProjectionForBothCoreTypes() {
    ExtensionFragment eventMedia = MultimediaMapping.chronometricAgeMediaForEvent(graph);
    ExtensionFragment occurrenceMedia = MultimediaMapping.chronometricAgeMediaForOccurrence(graph);

    assertEquals("event_fk", eventMedia.scopeKeyColumn().orElseThrow());
    assertEquals("occurrence_pk", occurrenceMedia.scopeKeyColumn().orElseThrow());
    assertTrue(
        eventMedia.relations().stream()
            .anyMatch(relation -> relation.targetResource().equals("chronometric-age-media")));
    assertTrue(
        occurrenceMedia.relations().stream()
            .anyMatch(relation -> relation.targetResource().equals("chronometric-age-media")));
  }

  @Test
  void canonicalOccurrencePlanContainsChronometricRows() {
    MappingPlan plan = OccurrenceDwcaMapping.current(graph);
    ExtensionMapping chronometric =
        plan.extensions().stream()
            .filter(extension -> extension.rowType().equals(ChronometricMapping.ROW_TYPE_CHRONOMETRIC_AGE))
            .findFirst()
            .orElseThrow();

    assertEquals(
        Set.of(
            "event-chronometric-age-for-occurrence",
            "event-chronometric-age-protocol-for-occurrence",
            "event-chronometric-age-conversion-protocol-for-occurrence",
            "event-chronometric-age-determined-by-for-occurrence"),
        chronometric.fragments().stream().map(ExtensionFragment::name).collect(Collectors.toSet()));
  }

}
