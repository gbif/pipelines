package org.gbif.pipelines.core.interpreters.model;

public interface IdentifierRecord {
    void setUniqueKey(String occurrenceId);
    void setInternalId(String sha1);
    String getInternalId();

    void setAssociatedKey(String tr);

    void addIssue(String gbifIdAbsent);

    String getUniqueKey();

    String getAssociatedKey();

    IssueRecord getIssues();
}
