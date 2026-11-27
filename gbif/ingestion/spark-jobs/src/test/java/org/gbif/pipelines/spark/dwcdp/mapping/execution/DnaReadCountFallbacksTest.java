package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.config.NucleotideMapping;
import org.junit.jupiter.api.Test;

class DnaReadCountFallbacksTest {
  private static final String QUANTITY = DwcTerm.organismQuantity.qualifiedName();
  private static final String QUANTITY_TYPE = DwcTerm.organismQuantityType.qualifiedName();
  private static final String SAMPLE_SIZE = DwcTerm.sampleSizeValue.qualifiedName();
  private static final String SAMPLE_UNIT = DwcTerm.sampleSizeUnit.qualifiedName();
  private static final String DNA = NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA;

  @Test
  void occurrenceCoreFillsOnlyMissingQuantityFromOneDistinctReadCount() {
    Map<String, List<Map<String, String>>> dna =
        Map.of(DNA, List.of(Map.of("readCount", "131"), Map.of("readCount", "131")));
    Map<String, String> occurrence = new HashMap<>();
    DnaReadCountFallbacks.apply(DwcTerm.Occurrence.qualifiedName(), occurrence, dna);
    assertEquals(Map.of(QUANTITY, "131", QUANTITY_TYPE, "DNA sequence reads"), occurrence);

    occurrence.put(QUANTITY, "9");
    occurrence.put(QUANTITY_TYPE, "individuals");
    DnaReadCountFallbacks.apply(DwcTerm.Occurrence.qualifiedName(), occurrence, dna);
    assertEquals(Map.of(QUANTITY, "9", QUANTITY_TYPE, "individuals"), occurrence);
  }

  @Test
  void conflictingCountsLeaveQuantitiesEmptyAndPublishedValuesArePreserved() {
    Map<String, String> occurrence = new HashMap<>();
    DnaReadCountFallbacks.apply(
        DwcTerm.Occurrence.qualifiedName(),
        occurrence,
        Map.of(DNA, List.of(Map.of("readCount", "131"), Map.of("readCount", "132"))));
    assertFalse(occurrence.containsKey(QUANTITY));
    assertFalse(occurrence.containsKey(QUANTITY_TYPE));

    Map<String, String> event = new HashMap<>();
    event.put(NucleotideMapping.EVENT_READ_TOTALS, "36207|36208");
    DnaReadCountFallbacks.apply(DwcTerm.Event.qualifiedName(), event, Map.of());
    assertEquals(Map.of(), event);

    event.put(SAMPLE_SIZE, "2");
    event.put(SAMPLE_UNIT, "litres");
    event.put(NucleotideMapping.EVENT_READ_TOTALS, "36207");
    DnaReadCountFallbacks.apply(DwcTerm.Event.qualifiedName(), event, Map.of());
    assertEquals(Map.of(SAMPLE_SIZE, "2", SAMPLE_UNIT, "litres"), event);
  }
}
