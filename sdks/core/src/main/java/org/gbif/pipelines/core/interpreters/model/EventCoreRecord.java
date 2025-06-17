package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface EventCoreRecord {
    void setEventType(VocabularyConcept vocabularyConcept);
    void addIssue(String eventIdToParentIdLoopingIssue);
    void setParentsLineage(List<Parent> parents);
}
