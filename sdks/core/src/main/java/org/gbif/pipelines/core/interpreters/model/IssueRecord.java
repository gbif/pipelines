package org.gbif.pipelines.core.interpreters.model;

import java.util.Collection;
import java.util.List;

public interface IssueRecord {
    Collection<String> getIssueList();
    void setIssueList(List<String> issues);
}
