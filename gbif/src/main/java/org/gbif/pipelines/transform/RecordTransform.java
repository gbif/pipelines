package org.gbif.pipelines.transform;

import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.Validation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Common class for a transformation record process
 * @param <T> transform "from" class
 * @param <R> transform "to" class
 */
public abstract class RecordTransform<T, R> extends PTransform<PCollection<T>, PCollectionTuple> {

  RecordTransform(String stepName) {
    this.stepName = stepName;
  }

  private final String stepName;
  private final TupleTag<R> dataTupleTag = new TupleTag<R>() {};
  private final TupleTag<OccurrenceIssue> issueTupleTag = new TupleTag<OccurrenceIssue>() {};

  @Override
  public PCollectionTuple expand(PCollection<T> input) {
    return input.apply(stepName, ParDo.of(interpret()).withOutputTags(dataTupleTag, TupleTagList.of(issueTupleTag)));
  }

  abstract DoFn<T, R> interpret();

  /**
   * @return all data, without and with issues
   */
  public TupleTag<R> getDataTupleTag() {
    return dataTupleTag;
  }

  /**
   * @return data only with issues
   */
  public TupleTag<OccurrenceIssue> getIssueTupleTag() {
    return issueTupleTag;
  }

  /**
   * Translates a OccurrenceIssue into Validation object.
   */
  static Validation toValidation(IssueType occurrenceIssue) {
    return Validation.newBuilder()
      .setName(occurrenceIssue.name())
      .setSeverity(occurrenceIssue.name())
      .build();
  }

}
