package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_NONE;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;

import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/**
 * Counterpart of {@link TaxonomyInterpreter} for DNA assisted classification: the name usage match
 * request is built from the sequence classifier result instead of from an ExtendedRecord. Lives in
 * the same package to reuse the protected {@link TaxonomyInterpreter#createTaxonRecord}, so the
 * matching, no-match handling and issues are exactly the same.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DnaTaxonomyInterpreter {

  /**
   * Interprets the taxonomy of a {@link ClassifyOccurrenceResponse} into the {@link TaxonRecord}.
   */
  public static BiConsumer<ClassifyOccurrenceResponse, TaxonRecord> dnaTaxonomyInterpreter(
      KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore, String checklistKey) {
    return (result, tr) -> {
      if (kvStore == null) {
        return;
      }

      // The ws echoes the gbifID of the occurrence it was given
      tr.setId(result.getGbifID());
      tr.setDatasetKey(checklistKey + "_dna_assisted");

      NameUsageMatchRequest request =
          createNameUsageMatchRequest(result.getClassification(), checklistKey);

      // No valid sequences, no match, or the ws failed for this occurrence: nothing to look up
      if (request == null) {
        addIssue(tr, TAXON_MATCH_NONE);
        return;
      }

      TaxonomyInterpreter.createTaxonRecord(request, kvStore, checklistKey, tr);
    };
  }

  /** Returns null if the classification has nothing to match on. */
  static NameUsageMatchRequest createNameUsageMatchRequest(
      ClassifyOccurrenceResponse.DnaClassification c, String checklistKey) {
    if (c == null) {
      return null;
    }

    String scientificName = trimToNull(c.getScientificName());
    String kingdom = trimToNull(c.getKingdom());
    String phylum = trimToNull(c.getPhylum());
    String clazz = trimToNull(c.getClazz());
    String order = trimToNull(c.getOrder());
    String family = trimToNull(c.getFamily());
    String genus = trimToNull(c.getGenus());

    if (scientificName == null
        && kingdom == null
        && phylum == null
        && clazz == null
        && order == null
        && family == null
        && genus == null) {
      return null;
    }

    return NameUsageMatchRequest.builder()
        .withChecklistKey(checklistKey)
        .withScientificName(scientificName)
        .withTaxonRank(trimToNull(c.getTaxonRank()))
        .withKingdom(kingdom)
        .withPhylum(phylum)
        .withClazz(clazz)
        .withOrder(order)
        .withFamily(family)
        .withGenus(genus)
        .build();
  }

  private static String trimToNull(String s) {
    if (s == null) {
      return null;
    }
    String t = s.trim();
    return t.isEmpty() ? null : t;
  }
}
