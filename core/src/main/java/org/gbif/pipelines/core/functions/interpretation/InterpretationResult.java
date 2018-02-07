package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;

import java.util.List;
import java.util.Optional;

public class InterpretationResult<T> {

  private Optional<T> result;
  private boolean isSuccessFull;
  private List<Issue> issueList;
  private List<Lineage> lineageList;

  InterpretationResult(){}

  public Optional<T> getResult() {
    return result;
  }

  public boolean isSuccessFull() {
    return isSuccessFull;
  }

  public List<Issue> getIssueList() {
    return issueList;
  }

  public List<Lineage> getLineageList() {
    return lineageList;
  }

  void setResult(Optional<T> result) {
    this.result = result;
  }

  void setSuccessFull(boolean successFull) {
    isSuccessFull = successFull;
  }

  void setIssueList(List<Issue> issueList) {
    this.issueList = issueList;
  }

  void setLineageList(List<Lineage> lineageList) {
    this.lineageList = lineageList;
  }

  @Override
  public String toString() {
    return "InterpretationResult{"
           + "result="
           + result
           + ", isSuccessFull="
           + isSuccessFull
           + ", issueList="
           + issueList
           + ", lineageList="
           + lineageList
           + '}';
  }
}
