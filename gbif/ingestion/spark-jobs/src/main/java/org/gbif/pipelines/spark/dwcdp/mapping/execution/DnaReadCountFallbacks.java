package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.config.NucleotideMapping;

/** Populates each quantity and its unit together after the DwC-DP rows have been attached. */
final class DnaReadCountFallbacks {
  private static final String OCCURRENCE_ID = DwcTerm.occurrenceID.qualifiedName();
  private static final String READ_UNIT = "DNA sequence reads";

  private DnaReadCountFallbacks() {}

  static void apply(
      String coreRowType,
      Map<String, String> core,
      Map<String, List<Map<String, String>>> extensions) {
    List<Map<String, String>> dnaRows =
        extensions.getOrDefault(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA, List.of());
    if (DwcTerm.Occurrence.qualifiedName().equals(coreRowType)) {
      Set<String> counts = new HashSet<>();
      for (Map<String, String> dnaRow : dnaRows) {
        addCount(counts, dnaRow.get("readCount"));
      }
      fillOccurrenceQuantity(core, counts);
      return;
    }

    String eventTotals = core.remove(NucleotideMapping.EVENT_READ_TOTALS);
    if (eventTotals != null && !eventTotals.contains("|")) {
      fill(core, DwcTerm.sampleSizeValue, DwcTerm.sampleSizeUnit, eventTotals);
    }

    Map<String, Set<String>> countsByOccurrence = new HashMap<>();
    for (Map<String, String> dnaRow : dnaRows) {
      String occurrenceId = dnaRow.get(OCCURRENCE_ID);
      if (occurrenceId != null) {
        addCount(
            countsByOccurrence.computeIfAbsent(occurrenceId, key -> new HashSet<>()),
            dnaRow.get("readCount"));
      }
    }
    for (Map<String, String> occurrence :
        extensions.getOrDefault(DwcTerm.Occurrence.qualifiedName(), List.of())) {
      fillOccurrenceQuantity(
          occurrence, countsByOccurrence.getOrDefault(occurrence.get(OCCURRENCE_ID), Set.of()));
    }
  }

  private static void addCount(Set<String> counts, String value) {
    if (value != null && !value.isBlank()) {
      counts.add(value.trim());
    }
  }

  private static void fillOccurrenceQuantity(Map<String, String> occurrence, Set<String> counts) {
    if (counts.size() == 1) {
      fill(
          occurrence,
          DwcTerm.organismQuantity,
          DwcTerm.organismQuantityType,
          counts.iterator().next());
    }
  }

  private static void fill(
      Map<String, String> terms, DwcTerm valueTerm, DwcTerm unitTerm, String value) {
    String published = terms.get(valueTerm.qualifiedName());
    if ((published == null || published.isBlank()) && !value.isBlank()) {
      terms.put(valueTerm.qualifiedName(), value.trim());
      terms.put(unitTerm.qualifiedName(), READ_UNIT);
    }
  }
}
