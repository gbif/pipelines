package org.gbif.pipelines.core.functions.interpretation;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * Validates the input, and provide appropriate interpretation, with list of issues and lineages (application of changes)
 */
public abstract class InterpretableDataFunction<T,V> extends DoFn {

  protected T rawData;
  protected V interpretedData;

  static TupleTag<String> issueTag = new TupleTag<>();
  static TupleTag<String> lineageTag = new TupleTag<>();
  static TupleTag<String> resultTag = new TupleTag<>();


  private List<Issue> issueList = new ArrayList<Issue>();
  private List<Lineage> lineage = new ArrayList<Lineage>();
  private boolean isDataChanged = false;

  public List<Issue> getIssueList() {
    return issueList;
  }

  public List<Lineage> getLineage() {
    return lineage;
  }

  public T getRawData() {
    return rawData;
  }

  public V getInterpretedData() {
    return interpretedData;
  }

  /**
   * if the raw value is updated
   * @return true if raw value is updated or cannot be interpreted
   */
  public boolean isDataChanged() {
    return isDataChanged;
  }

  /**
   * validate the input data for any issues and adds the issues in issueList.
   * @return if validation is passed true else fail.
   */
  protected abstract boolean validate(ProcessContext context);

  /**
   * apply the interpreted value and set lineage in case of changes
   * @param c
   */
  protected abstract void apply(ProcessContext context);

  /**
   * the main function to validate and update the input with issues and lineage, cannot be extended.
   * @param c
   */
  @ProcessElement
  public final void processElement(ProcessContext c){
    if(!validate(c))
      isDataChanged = true;
    apply(c);
    c.outputWithTimestamp(issueTag, getIssueList(), Instant.now());
    c.outputWithTimestamp(lineageTag, getLineage(), Instant.now());
  }




}
