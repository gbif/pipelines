package org.gbif.pipelines.transform;

import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.Validation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Common class for a transform recocd process
 *
 * @param <T> transform "from" class
 * @param <R> transform "to" class
 */
public abstract class RecordTransform<T, R> extends PTransform<PCollection<T>, PCollectionTuple> {

  RecordTransform(String stepName) {
    this.stepName = stepName;
  }

  private final String stepName;
  private final TupleTag<KV<String, R>> dataTag = new TupleTag<KV<String, R>>() {};
  private final TupleTag<KV<String, OccurrenceIssue>> issueTag = new TupleTag<KV<String, OccurrenceIssue>>() {};

  @Override
  public PCollectionTuple expand(PCollection<T> input) {
    return input.apply(stepName, ParDo.of(interpret()).withOutputTags(dataTag, TupleTagList.of(issueTag)));
  }

  abstract DoFn<T, KV<String, R>> interpret();

  /**
   * @return all data, without and with issues
   */
  public TupleTag<KV<String, R>> getDataTag() {
    return dataTag;
  }

  /**
   * @return data only with issues
   */
  public TupleTag<KV<String, OccurrenceIssue>> getIssueTag() {
    return issueTag;
  }

  /**
   * Translates a OccurrenceIssue into Validation object.
   */
  static Validation toValidation(IssueType issueType) {
    return Validation.newBuilder()
      .setName(issueType.name())
      .build();
  }

}
