package org.gbif.pipelines.core.interpreter.taxonomy;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class InterpretedTaxonomy implements Serializable {

  private static final long serialVersionUID = 5754733108334628580L;

  private TaxonRecord taxonRecord;

  // FIXME: change when the issue handling approach is defined
  private List<OccurrenceIssue> issues = new ArrayList<>();

  public TaxonRecord getTaxonRecord() {
    return taxonRecord;
  }

  public void setTaxonRecord(TaxonRecord taxonRecord) {
    this.taxonRecord = taxonRecord;
  }

  public List<OccurrenceIssue> getIssues() {
    return issues;
  }

  public void setIssues(List<OccurrenceIssue> issues) {
    this.issues = issues;
  }
}
