package org.gbif.pipelines.core.interpreters.model;

import java.util.Collection;

public interface VocabularyConcept {

    String getConcept();

    Collection<VocabularyTag> getTags();
}
