package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface AudubonRecord {
    String getId();
    void setId(String id);

    Long getCreated();
    void setCreated(Long created);

    List<Audubon> getAudubonItems();
    void setAudubonItems(List<Audubon> audubonItems);

    IssueRecord getIssues(); // Replace with interface/class as needed
    void setIssues(IssueRecord issues);
}
