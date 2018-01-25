package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * This transform is the main code which connects several functions of converting the raw record to interpreted record.
 * This transform takes the PCollection of ExtendedRecord and returns PCollectionTuple which are tagged with appropriate category of interpreted data and issue/lineages.
 */
public class RawToInterpretedCategoryTransformer extends PTransform<PCollection<ExtendedRecord>,PCollectionTuple>{

  /**
   * tags for the final output tuple indicating the type of collection and its category
   */
  public static final TupleTag<KV<String,Event>> temporalCategory = new TupleTag<KV<String,Event>>() {};
  public static final TupleTag<KV<String,Location>> spatialCategory = new TupleTag<KV<String,Location>>() {};
  public static final TupleTag<KV<String,IssueLineageRecord>> temporalCategoryIssues = new TupleTag<KV<String,IssueLineageRecord>>() {};
  public static final TupleTag<KV<String,IssueLineageRecord>> spatialCategoryIssues = new TupleTag<KV<String,IssueLineageRecord>>() {};


  /**
   * Override this method to specify how this {@code PTransform} should be expanded
   * on the given {@code InputT}.
   * <p>
   * <p>NOTE: This method should not be called directly. Instead apply the
   * {@code PTransform} should be applied to the {@code InputT} using the {@code apply}
   * method.
   * <p>
   * <p>Composite transforms, which are defined in terms of other transforms,
   * should return the output of one of the composed transforms.  Non-composite
   * transforms, which do not apply any transforms internally, should return
   * a new unbound output and register evaluators (via backend-specific
   * registration methods).
   */
  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {
    //get the multiple output tuple from raw to interpreted temporal record along with issues.
    PCollectionTuple eventView = input.apply(ParDo.of(new ExtendedRecordToEventTransformer()).withOutputTags(ExtendedRecordToEventTransformer.eventDataTag,
                                                                                                             TupleTagList
                                                                                                               .of(ExtendedRecordToEventTransformer.eventIssueTag)));
    //get the multiple output tuple from raw to interpreted spatial record along with issues.
    PCollectionTuple locationView = input.apply(ParDo.of(new ExtendedRecordToLocationTransformer()).withOutputTags(ExtendedRecordToLocationTransformer.locationDataTag,TupleTagList.of(ExtendedRecordToLocationTransformer.locationIssueTag)));
    //combining the different collections as one tuple
    final PCollectionTuple resultantTuples = PCollectionTuple.of(temporalCategory, eventView.get(ExtendedRecordToEventTransformer.eventDataTag)).and(
      spatialCategory, locationView.get(ExtendedRecordToLocationTransformer.locationDataTag))
      .and(temporalCategoryIssues, eventView.get(ExtendedRecordToEventTransformer.eventIssueTag)).and(
        spatialCategoryIssues, locationView.get(ExtendedRecordToLocationTransformer.locationIssueTag));
    return resultantTuples;
  }
}
