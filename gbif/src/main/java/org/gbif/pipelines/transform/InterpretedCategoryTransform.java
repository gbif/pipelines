package org.gbif.pipelines.transform;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transform.function.EventTransform;
import org.gbif.pipelines.transform.function.LocationTransform;

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
public class InterpretedCategoryTransform extends PTransform<PCollection<ExtendedRecord>, PCollectionTuple> {

  //next 2 types are identical, do we need 2?
  private static final TupleTag<KV<String, IssueLineageRecord>> TEMPORAL_CATEGORY_ISSUES =
    new TupleTag<KV<String, IssueLineageRecord>>() {};
  private static final TupleTag<KV<String, IssueLineageRecord>> SPATIAL_CATEGORY_ISSUES =
    new TupleTag<KV<String, IssueLineageRecord>>() {};
  /**
   * tags for the final output tuple indicating the type of collection and its category
   */
  private final TupleTag<KV<String, Event>> temporalCategory = new TupleTag<KV<String, Event>>() {};
  private final TupleTag<KV<String, Location>> spatialCategory = new TupleTag<KV<String, Location>>() {};

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
    EventTransform eventTransformer = new EventTransform();
    PCollectionTuple event = input.apply(ParDo.of(eventTransformer)
                                           .withOutputTags(eventTransformer.getEventDataTag(),
                                                           TupleTagList.of(eventTransformer.getEventIssueTag())));
    //get the multiple output tuple from raw to interpreted spatial record along with issues.
    LocationTransform locationTransformer = new LocationTransform();
    PCollectionTuple location = input.apply(ParDo.of(locationTransformer)
                                              .withOutputTags(locationTransformer.getLocationDataTag(),
                                                              TupleTagList.of(locationTransformer.getLocationIssueTag())));
    //combining the different collections as one tuple
    return PCollectionTuple.of(temporalCategory, event.get(eventTransformer.getEventDataTag()))
      .and(spatialCategory, location.get(locationTransformer.getLocationDataTag()))
      .and(TEMPORAL_CATEGORY_ISSUES, event.get(eventTransformer.getEventIssueTag()))
      .and(SPATIAL_CATEGORY_ISSUES, location.get(locationTransformer.getLocationIssueTag()));
  }

  public TupleTag<KV<String, Event>> getTemporalCategory() {
    return temporalCategory;
  }

  public TupleTag<KV<String, Location>> getSpatialCategory() {
    return spatialCategory;
  }

  public TupleTag<KV<String, IssueLineageRecord>> getTemporalCategoryIssues() {
    return TEMPORAL_CATEGORY_ISSUES;
  }

  public TupleTag<KV<String, IssueLineageRecord>> getSpatialCategoryIssues() {
    return SPATIAL_CATEGORY_ISSUES;
  }
}
