package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;

import java.util.ArrayList;
import java.util.List;

/**
 * Interpretation Exception with issues and lineages
 * also set interpretedObject in case you had one
 */
public class InterpretationException extends Exception {

  private List<Issue> issues = new ArrayList<>();
  private List<Lineage> lineages = new ArrayList<>();
  private Object interpretedValue;
  private boolean isInterpretedObjectAvailable = false;

  public InterpretationException(List<Issue> issues, List<Lineage> lineages) {
    this.issues = issues;
    this.lineages = lineages;
  }

  public List<Issue> getIssues() {
    return issues;
  }

  public List<Lineage> getLineages() {
    return lineages;
  }

  public Object getInterpretedValue() {
    return interpretedValue;
  }

  public void setInterpretedValue(Object interpretedValue) {
    isInterpretedObjectAvailable = true;
    this.interpretedValue = interpretedValue;
  }

  public boolean isInterpretedObjectSet() {
    return isInterpretedObjectAvailable;
  }

}
