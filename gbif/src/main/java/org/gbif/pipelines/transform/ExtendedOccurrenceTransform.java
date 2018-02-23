package org.gbif.pipelines.transform;

import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurrence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.transform.function.InterpretedIssueRecordTransform;
import org.gbif.pipelines.transform.function.InterpretedOccurrenceTransform;

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
public class ExtendedOccurrenceTransform extends PTransform<PCollectionTuple, PCollectionTuple> {

  private final TupleTag<Event> tupleTag = new TupleTag<Event>() {};
  private final TupleTag<Location> spatialTag = new TupleTag<Location>() {};
  private final TupleTag<IssueLineageRecord> temporalIssueTag = new TupleTag<IssueLineageRecord>() {};
  private final TupleTag<IssueLineageRecord> spatialIssueTag = new TupleTag<IssueLineageRecord>() {};

  private final TupleTag<ExtendedOccurrence> interpretedOccurrence = new TupleTag<ExtendedOccurrence>() {};
  private final TupleTag<IssueLineageRecord> interpretedIssue = new TupleTag<IssueLineageRecord>() {};

  private final InterpretedCategoryTransform transformer;

  public ExtendedOccurrenceTransform(InterpretedCategoryTransform transformer) {
    this.transformer = transformer;
  }

  @Override
  public PCollectionTuple expand(PCollectionTuple interpretedCategory) {
    /*
      Joining temporal category issues and spatial category issues to get the overall issues together.
     */
    PCollection<KV<String, CoGbkResult>> joinedIssueCollection =
      KeyedPCollectionTuple.of(temporalIssueTag, interpretedCategory.get(transformer.getTemporalCategoryIssues()))
        .and(spatialIssueTag, interpretedCategory.get(transformer.getSpatialCategoryIssues()))
        .apply(CoGroupByKey.create());

    PCollection<IssueLineageRecord> interpretedIssueLineageRecords = joinedIssueCollection.apply(
      "Applying join on the issues and lineages obtained",
      ParDo.of(new InterpretedIssueRecordTransform(this)));

    /*
      Joining temporal category and spatial category to get the big flat interpreted record.
     */
    PCollection<KV<String, CoGbkResult>> joinedCollection =
      KeyedPCollectionTuple.of(tupleTag, interpretedCategory.get(transformer.getTemporalCategory()))
        .and(spatialTag, interpretedCategory.get(transformer.getSpatialCategory()))
        .apply(CoGroupByKey.create());

    PCollection<ExtendedOccurrence> interpretedRecords = joinedCollection.apply(
      "Applying join on interpreted category of records to make a flat big interpreted record",
      ParDo.of(new InterpretedOccurrenceTransform(this)));

    return PCollectionTuple.of(interpretedOccurrence, interpretedRecords)
      .and(interpretedIssue, interpretedIssueLineageRecords);
  }

  public TupleTag<Event> getTemporalTag() {
    return tupleTag;
  }

  public TupleTag<Location> getSpatialTag() {
    return spatialTag;
  }

  public TupleTag<IssueLineageRecord> getTemporalIssueTag() {
    return temporalIssueTag;
  }

  public TupleTag<IssueLineageRecord> getSpatialIssueTag() {
    return spatialIssueTag;
  }

  public TupleTag<ExtendedOccurrence> getInterpretedOccurrence() {
    return interpretedOccurrence;
  }

  public TupleTag<IssueLineageRecord> getInterpretedIssue() {
    return interpretedIssue;
  }

}
