package org.gbif.pipelines.transforms;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.transforms.function.InterpretedIssueRecordTransform;
import org.gbif.pipelines.transforms.function.InterpretedOccurenceTransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Transform Joins the Individual categories via occurenceId to create ExtendedOccurrence
 */
public class ExtendedOccurenceTransform extends PTransform<PCollectionTuple, PCollectionTuple> {

  private final TupleTag<Event> TEMPORAL_TAG = new TupleTag<Event>() {};
  private final TupleTag<Location> SPATIAL_TAG = new TupleTag<Location>() {};
  private final TupleTag<IssueLineageRecord> TEMPORAL_ISSUE_TAG = new TupleTag<IssueLineageRecord>() {};
  private final TupleTag<IssueLineageRecord> SPATIAL_ISSUE_TAG = new TupleTag<IssueLineageRecord>() {};

  private final TupleTag<ExtendedOccurence> INTERPRETED_OCCURENCE = new TupleTag<ExtendedOccurence>() {};
  private final TupleTag<IssueLineageRecord> INTERPRETED_ISSUE = new TupleTag<IssueLineageRecord>() {};

  private final InterpretedCategoryTransform transformer;

  public ExtendedOccurenceTransform(
    InterpretedCategoryTransform transformer
  ) {
    this.transformer = transformer;
  }

  @Override
  public PCollectionTuple expand(PCollectionTuple interpretedCategory) {
    /*
      Joining temporal category issues and spatial category issues to get the overall issues together.
     */
    PCollection<KV<String, CoGbkResult>> joinedIssueCollection =
      KeyedPCollectionTuple.of(TEMPORAL_ISSUE_TAG, interpretedCategory.get(transformer.getTemporalCategoryIssues()))
        .and(SPATIAL_ISSUE_TAG, interpretedCategory.get(transformer.getSpatialCategoryIssues()))
        .apply(CoGroupByKey.create());

    PCollection<IssueLineageRecord> interpretedIssueLineageRecords = joinedIssueCollection.apply(
      "Applying join on the issues and lineages obtained",
      ParDo.of(new InterpretedIssueRecordTransform(this)));

    /*
      Joining temporal category and spatial category to get the big flat interpreted record.
     */
    PCollection<KV<String, CoGbkResult>> joinedCollection =
      KeyedPCollectionTuple.of(TEMPORAL_TAG, interpretedCategory.get(transformer.getTemporalCategory()))
        .and(SPATIAL_TAG, interpretedCategory.get(transformer.getSpatialCategory()))
        .apply(CoGroupByKey.create());

    PCollection<ExtendedOccurence> interpretedRecords = joinedCollection.apply(
      "Applying join on interpreted category of records to make a flat big interpreted record",
      ParDo.of(new InterpretedOccurenceTransform(this)));

    return PCollectionTuple.of(INTERPRETED_OCCURENCE, interpretedRecords)
      .and(INTERPRETED_ISSUE, interpretedIssueLineageRecords);
  }

  public TupleTag<Event> getTemporalTag() {
    return TEMPORAL_TAG;
  }

  public TupleTag<Location> getSpatialTag() {
    return SPATIAL_TAG;
  }

  public TupleTag<IssueLineageRecord> getTemporalIssueTag() {
    return TEMPORAL_ISSUE_TAG;
  }

  public TupleTag<IssueLineageRecord> getSpatialIssueTag() {
    return SPATIAL_ISSUE_TAG;
  }

  public TupleTag<ExtendedOccurence> getInterpretedOccurence() {
    return INTERPRETED_OCCURENCE;
  }

  public TupleTag<IssueLineageRecord> getInterpretedIssue() {
    return INTERPRETED_ISSUE;
  }

}
