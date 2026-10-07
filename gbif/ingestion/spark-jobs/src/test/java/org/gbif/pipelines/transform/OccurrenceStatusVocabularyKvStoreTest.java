package org.gbif.pipelines.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.transform.BasicTransform.OccurrenceStatusVocabularyKvStore;
import org.gbif.vocabulary.lookup.InMemoryVocabularyLookup;
import org.junit.Test;

public class OccurrenceStatusVocabularyKvStoreTest {

  private static VocabularyService service() {
    return VocabularyService.builder()
        .vocabularyLookup(
            DwcTerm.occurrenceStatus.qualifiedName(),
            InMemoryVocabularyLookup.newBuilder()
                .from(
                    Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream("vocab/OccurrenceStatus.json"))
                .build())
        .build();
  }

  @Test
  public void lookupDeprecatedPresentResolvesToDetected() {
    OccurrenceStatusVocabularyKvStore store = OccurrenceStatusVocabularyKvStore.create(service());
    assertEquals(OccurrenceStatus.DETECTED, store.get("present"));
    assertEquals(OccurrenceStatus.DETECTED, store.get("PRESENT"));
    assertEquals(OccurrenceStatus.DETECTED, store.get("Detected"));
  }

  @Test
  public void lookupDeprecatedAbsentResolvesToNotDetected() {
    OccurrenceStatusVocabularyKvStore store = OccurrenceStatusVocabularyKvStore.create(service());
    assertEquals(OccurrenceStatus.NOT_DETECTED, store.get("absent"));
    assertEquals(OccurrenceStatus.NOT_DETECTED, store.get("ABSENT"));
    assertEquals(OccurrenceStatus.NOT_DETECTED, store.get("NotDetected"));
  }

  @Test
  public void lookupUnknownReturnsNull() {
    OccurrenceStatusVocabularyKvStore store = OccurrenceStatusVocabularyKvStore.create(service());
    assertNull(store.get("blabla"));
  }

  @Test
  public void conceptNameMapping() {
    assertEquals(
        OccurrenceStatus.DETECTED,
        OccurrenceStatusVocabularyKvStore.toOccurrenceStatus("Detected"));
    assertEquals(
        OccurrenceStatus.DETECTED, OccurrenceStatusVocabularyKvStore.toOccurrenceStatus("Present"));
    assertEquals(
        OccurrenceStatus.NOT_DETECTED,
        OccurrenceStatusVocabularyKvStore.toOccurrenceStatus("NotDetected"));
    assertEquals(
        OccurrenceStatus.NOT_DETECTED,
        OccurrenceStatusVocabularyKvStore.toOccurrenceStatus("Absent"));
    assertNull(OccurrenceStatusVocabularyKvStore.toOccurrenceStatus("unknown"));
  }
}
