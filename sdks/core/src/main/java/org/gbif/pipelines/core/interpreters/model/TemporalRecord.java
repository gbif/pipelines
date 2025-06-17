package org.gbif.pipelines.core.interpreters.model;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

public interface TemporalRecord {
    void setYear(Integer year);

    void setMonth(Integer month);

    void setDay(Integer day);

    void setStartDayOfYear(int i);

    void setEndDayOfYear(int i);

    void setEventDate(EventDate ed);

    void addIssueSet(Set<OccurrenceIssue> issues);

    void setModified(@NotNull String s);

    void setDateIdentified(@NotNull String s);

    void setParentId(@NotNull String s);
}
