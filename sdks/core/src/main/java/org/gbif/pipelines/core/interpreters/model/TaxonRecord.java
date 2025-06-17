package org.gbif.pipelines.core.interpreters.model;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

public interface TaxonRecord {
    void setUsage(RankedNameWithAuthorship usage);

    void setId(String id);

    void setDatasetKey(String checklistKey);

    void setClassification(List<RankedName> objects);

    void addIssue(OccurrenceIssue occurrenceIssue);

    void setCoreId(@NotNull String s);

    void setParentId(@NotNull String s);

    void addIssueSet(Set<OccurrenceIssue> collect);

    void setSynonym(boolean synonym);
}
