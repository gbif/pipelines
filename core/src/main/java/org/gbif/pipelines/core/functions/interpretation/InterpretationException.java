package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Interpretation Exception with issues and lineages
 * also set interpretedObject in case you had one
 */
public class InterpretationException extends Exception {

  private List<Issue> issues = new ArrayList<>();
  private List<Lineage> lineages = new ArrayList<>();
  private Optional<Object> interpretedValue = Optional.empty();

  public InterpretationException(List<Issue> issues, List<Lineage> lineages, Object interpretedValue) {
    this.issues = issues;
    this.lineages = lineages;
    setInterpretedValue(interpretedValue);
  }

  public InterpretationException() {}

  /**
   * get all issues
   */
  public List<Issue> getIssues() {
    return issues;
  }

  /**
   * get lineage of change in data
   */
  public List<Lineage> getLineages() {
    return lineages;
  }

  /**
   * get interpreted value
   */

  public Optional<Object> getInterpretedValue() {
    return interpretedValue;
  }

  public void setInterpretedValue(Object interpretedValue) {
    this.interpretedValue = Optional.ofNullable(interpretedValue);
  }

  public void addIssue(IssueType issueType, String remarks) {
    issues.add(Issue.newBuilder().setIssueType(issueType).setRemark(remarks).build());
  }

  public void addLineage(LineageType lineageType, String remarks) {
    lineages.add(Lineage.newBuilder().setLineageType(lineageType).setRemark(remarks).build());
  }
}
