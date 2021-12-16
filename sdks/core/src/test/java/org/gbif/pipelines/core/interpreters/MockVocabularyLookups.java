package org.gbif.pipelines.core.interpreters;

import java.util.ArrayList;
import java.util.Optional;
import org.gbif.vocabulary.lookup.LookupConcept;
import org.gbif.vocabulary.lookup.VocabularyLookup;
import org.gbif.vocabulary.model.Concept;
import org.gbif.vocabulary.model.LanguageRegion;

public class MockVocabularyLookups {

  public static class LifeStageMockVocabularyLookup implements VocabularyLookup {

    @Override
    public Optional<LookupConcept> lookup(String s) {
      return lookup(s, null);
    }

    @Override
    public Optional<LookupConcept> lookup(String s, LanguageRegion languageRegion) {
      if (s.equalsIgnoreCase("adult")) {
        Concept concept = new Concept();
        concept.setName("Adult");
        LookupConcept lookupConcept = LookupConcept.of(concept, new ArrayList<>(1));

        return Optional.of(lookupConcept);
      }
      return Optional.empty();
    }

    @Override
    public void close() {}
  }
}
