package org.gbif.pipelines.core.interpreters.model;

public interface AmplificationRecord {


    String getId();
    void setId(String id);

    Long getCreated();
    void setCreated(Long created);

    List<Amplification> getAmplificationItems();
    void setAmplificationItems(List<Amplification> amplificationItems);

    IssueRecord getIssues();
    void setIssues(IssueRecord issues);
}
