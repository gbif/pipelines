package org.gbif.pipelines.transform;

import org.gbif.dwca.avro.ExtendedOccurrence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.Validation;
import org.gbif.pipelines.mapper.ExtendedOccurrenceMapper;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 *
 */
public class ExtendedOccurrenceTransform extends RecordTransform<ExtendedRecord, ExtendedOccurrence> {

  private static final String DATA_STEP_NAME = "Interpret ExtendedOccurrence record";
  private static final String ISSUE_STEP_NAME = "Interpret ExtendedOccurrence issue";

  // Data tupple tags for internal usage only
  private final TupleTag<InterpretedExtendedRecord> recordDataTag = new TupleTag<InterpretedExtendedRecord>() {};
  private final TupleTag<Location> locationDataTag = new TupleTag<Location>() {};
  private final TupleTag<TemporalRecord> temporalDataTag = new TupleTag<TemporalRecord>() {};
  // Issue tupple tags for internal usage only
  private final TupleTag<OccurrenceIssue> recordIssueTag = new TupleTag<OccurrenceIssue>() {};
  private final TupleTag<OccurrenceIssue> locationIssueTag = new TupleTag<OccurrenceIssue>() {};
  private final TupleTag<OccurrenceIssue> temporalIssueTag = new TupleTag<OccurrenceIssue>() {};

  public ExtendedOccurrenceTransform() {
    super(DATA_STEP_NAME);
  }

  /**
   *
   */
  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {

    // STEP 1: Collect all records
    // Collect ExtendedRecord
    InterpretedExtendedRecordTransform recordTransform = new InterpretedExtendedRecordTransform();
    PCollectionTuple recordTupple = input.apply(recordTransform);

    // Collect Location
    LocationTransform locationTransform = new LocationTransform();
    PCollectionTuple locationTuple = input.apply(locationTransform);

    // Collect TemporalRecord
    TemporalRecordTransform temporalTransform = new TemporalRecordTransform();
    PCollectionTuple temporalTupple = input.apply(temporalTransform);

    // STEP 2: Group records and issues by key
    // Group records collections
    PCollection<KV<String, CoGbkResult>> groupedData =
      KeyedPCollectionTuple.of(recordDataTag, recordTupple.get(recordTransform.getDataTag()))
        .and(locationDataTag, locationTuple.get(locationTransform.getDataTag()))
        .and(temporalDataTag, temporalTupple.get(temporalTransform.getDataTag()))
        .apply(CoGroupByKey.create());

    // Group records issue collections
    PCollection<KV<String, CoGbkResult>> groupedIssue =
      KeyedPCollectionTuple.of(recordIssueTag, recordTupple.get(recordTransform.getIssueTag()))
        .and(locationIssueTag, locationTuple.get(locationTransform.getIssueTag()))
        .and(temporalIssueTag, temporalTupple.get(temporalTransform.getIssueTag()))
        .apply(CoGroupByKey.create());

    // Map ExtendedOccurrence records
    PCollection<KV<String, ExtendedOccurrence>> occurrenceCollection =
      groupedData.apply(DATA_STEP_NAME, mapOccurrenceParDo());

    // Map ExtendedOccurrence issues
    PCollection<KV<String, OccurrenceIssue>> issueCollection = groupedIssue.apply(ISSUE_STEP_NAME, mapIssueParDo());

    // Return data and issue
    return PCollectionTuple.of(getDataTag(), occurrenceCollection).and(getIssueTag(), issueCollection);
  }

  /**
   *
   */
  private ParDo.SingleOutput<KV<String, CoGbkResult>, KV<String, OccurrenceIssue>> mapIssueParDo() {
    return ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, OccurrenceIssue>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, CoGbkResult> element = c.element();

        CoGbkResult value = element.getValue();

        OccurrenceIssue recordIssue = value.getOnly(recordIssueTag);
        OccurrenceIssue locationIssue = value.getOnly(locationIssueTag);
        OccurrenceIssue temporalIssue = value.getOnly(temporalIssueTag);

        List<Validation> recordIssueList = cast(recordIssue.getIssues());
        List<Validation> locationIssueList = cast(locationIssue.getIssues());
        List<Validation> temporalIssueList = cast(temporalIssue.getIssues());

        int size = recordIssueList.size() + locationIssueList.size() + temporalIssueList.size();
        List<Validation> validations = new ArrayList<>(size);
        validations.addAll(recordIssueList);
        validations.addAll(locationIssueList);
        validations.addAll(temporalIssueList);

        OccurrenceIssue issues = OccurrenceIssue.newBuilder().setId(recordIssue.getId()).setIssues(validations).build();

        c.output(KV.of(element.getKey(), issues));
      }
    });
  }

  /**
   *
   */
  private ParDo.SingleOutput<KV<String, CoGbkResult>, KV<String, ExtendedOccurrence>> mapOccurrenceParDo() {
    return ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, ExtendedOccurrence>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, CoGbkResult> element = c.element();

        CoGbkResult value = element.getValue();

        InterpretedExtendedRecord record = value.getOnly(recordDataTag);
        Location location = value.getOnly(locationDataTag);
        TemporalRecord temporal = value.getOnly(temporalDataTag);

        ExtendedOccurrence occurrence = ExtendedOccurrenceMapper.map(record, location, temporal);

        c.output(KV.of(element.getKey(), occurrence));
      }
    });
  }

  @Override
  DoFn<ExtendedRecord, KV<String, ExtendedOccurrence>> interpret() {
    throw new UnsupportedOperationException("The method is not implemented");
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(final Object o) {
    return (T) o;
  }

}
