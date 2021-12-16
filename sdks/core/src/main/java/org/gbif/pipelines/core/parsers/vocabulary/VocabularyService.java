package org.gbif.pipelines.core.parsers.vocabulary;

import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Singular;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.Terms;
import org.gbif.vocabulary.lookup.VocabularyLookup;

@SuppressWarnings("FallThrough")
@Builder
public class VocabularyService {

  @Singular private final Map<Term, VocabularyLookup> vocabularyLookups;

  public Optional<VocabularyLookup> get(Term term) {
    if (!Terms.getVocabularyBackedTerms().contains(term)) {
      throw new IllegalArgumentException("Vocabulary-backed term not supported: " + term);
    }

    return Optional.ofNullable(vocabularyLookups.get(term));
  }

  public void close() {
    vocabularyLookups.values().forEach(VocabularyLookup::close);
  }
}
