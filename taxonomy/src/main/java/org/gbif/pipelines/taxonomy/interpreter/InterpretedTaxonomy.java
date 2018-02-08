package org.gbif.pipelines.taxonomy.interpreter;

import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.io.Serializable;

/**
 * Models the interpretation of a taxonomy.
 */
public class InterpretedTaxonomy implements Serializable {

  private static final long serialVersionUID = 5754733108334628580L;

  private TaxonRecord taxonRecord;

  private OccurrenceIssue occurrenceIssue;

  public TaxonRecord getTaxonRecord() {
    return taxonRecord;
  }

  public void setTaxonRecord(TaxonRecord taxonRecord) {
    this.taxonRecord = taxonRecord;
  }

  public OccurrenceIssue getOccurrenceIssue() {
    return occurrenceIssue;
  }

  public void setOccurrenceIssue(OccurrenceIssue occurrenceIssue) {
    this.occurrenceIssue = occurrenceIssue;
  }
}
