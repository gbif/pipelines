package org.gbif.pipelines.core.parsers;

import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.IssueType;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Models an issue found during the interpretation process. It links the issue with the
 * {@link org.gbif.dwc.terms.DwcTerm} associated that caused the issue.
 */
public class InterpretationIssue {

  private final List<Term> terms;
  private final IssueType issueType;

  public InterpretationIssue(IssueType issueType, List<Term> terms) {
    this.terms = terms;
    this.issueType = issueType;
  }

  public InterpretationIssue(IssueType issueType, Term... terms) {
    this.terms = Arrays.asList(terms);
    this.issueType = issueType;
  }

  public List<Term> getTerms() {
    return ImmutableList.copyOf(terms);
  }

  public IssueType getIssueType() {
    return issueType;
  }
}
