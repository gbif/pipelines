package org.gbif.pipelines.core.interpreters.model;

import org.gbif.api.vocabulary.OccurrenceIssue;

public interface GrscicollRecord {
    void addIssue(OccurrenceIssue institutionMatchNoneIssue);
    void setCollectionMatch(Match match);
    void setId(Object id);
    void setInstitutionMatch(Match match);
}
